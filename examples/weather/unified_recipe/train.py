# Copyright (c) 2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import torch
import os
import hydra
import wandb
import matplotlib.pyplot as plt
import time
import fsspec
import zarr
from omegaconf import DictConfig, OmegaConf
from torch.nn.parallel import DistributedDataParallel
from torch.cuda.amp import GradScaler
from torch.optim.lr_scheduler import SequentialLR
from torch import nn
from tqdm import tqdm

# Add eval to OmegaConf TODO: Remove when OmegaConf is updated
OmegaConf.register_new_resolver("eval", eval)

try:
    from apex import optimizers
except:
    raise ImportError(
        "training requires apex package for optimizer."
        + "See https://github.com/nvidia/apex for install details."
    )

from modulus import Module
from modulus.distributed import DistributedManager
from modulus.utils import StaticCaptureTraining, StaticCaptureEvaluateNoGrad
from modulus.launch.logging import (
    LaunchLogger,
    PythonLogger,
    initialize_mlflow,
    RankZeroLoggingWrapper,
)
from modulus.launch.utils import load_checkpoint, save_checkpoint
from modulus.metrics.general.mse import batch_normalized_mse

from seq_zarr_datapipe import SeqZarrDatapipe



@hydra.main(version_base="1.2", config_path="conf", config_name="config")
def main(cfg: DictConfig) -> None:

    # Resolve config so that all values are concrete
    OmegaConf.resolve(cfg)

    # Initialize distributed environment for training
    DistributedManager.initialize()
    dist = DistributedManager()

    # Initialize loggers
    initialize_mlflow(
        experiment_name="Modulus-Launch-Dev",
        experiment_desc="Modulus launch development",
        run_name=f"{cfg.model.name}-trainng",
        run_desc="Forcast Model Training",
        user_name="Modulus User",
        mode="offline",
    )
    LaunchLogger.initialize(use_mlflow=True)  # Modulus launch logger
    logger = PythonLogger("main")  # General python logger
    rank_zero_logger = RankZeroLoggingWrapper(logger, dist)  # Rank 0 logger

    # Initialize model
    model = Module.instantiate(
        {
            "__name__": cfg.model.name,
            "__args__": cfg.model.args,
        }
    )
    model = model.to(dist.device)

    # Distributed learning
    if dist.world_size > 1:
        ddps = torch.cuda.Stream()
        with torch.cuda.stream(ddps):
            model = DistributedDataParallel(
                model,
                device_ids=[dist.local_rank],
                output_device=dist.device,
                broadcast_buffers=dist.broadcast_buffers,
                find_unused_parameters=dist.find_unused_parameters,
            )
        torch.cuda.current_stream().wait_stream(ddps)
        torch.device.synchronize()

    # Initialize optimizer
    OptimizerClass = getattr(optimizers, cfg.training.optimizer.name)
    optimizer = OptimizerClass(model.parameters(), **cfg.training.optimizer.args)

    # Initialize scheduler
    schedulers = []
    milestones = []
    for scheduler_cfg in cfg.training.schedulers:
        SchedulerClass = getattr(torch.optim.lr_scheduler, scheduler_cfg.name)
        schedulers.append(SchedulerClass(optimizer, **scheduler_cfg.args))
        if not milestones:
            milestones.append(scheduler_cfg.num_iterations)
        else:
            milestones.append(milestones[-1] + scheduler_cfg.num_iterations)
    milestones.pop(-1)
    scheduler = SequentialLR(
        optimizer,
        schedulers=schedulers,
        milestones=milestones,
    )

    # Attempt to load latest checkpoint if one exists
    if dist.world_size > 1:
        torch.distributed.barrier()
    loaded_epoch = load_checkpoint(
        "./checkpoints",
        models=model,
        optimizer=optimizer,
        scheduler=scheduler,
        device=dist.device,
    )

    # Initialize filesytem
    if cfg.filesystem.type == 'file':
        fs = fsspec.filesystem(cfg.filesystem.type)
    elif cfg.filesystem.type == 's3':
        fs = fsspec.filesystem(cfg.filesystem.type,
                               key=cfg.filesystem.key,
                               secret=os.environ["AWS_SECRET_ACCESS_KEY"], 
                               client_kwargs={'endpoint_url': cfg.filesystem.endpoint_url,
                                              'region_name': cfg.filesystem.region_name})
    else:
        raise NotImplementedError(f'Filesystem type {cfg.filesystem.type} not implemented')

    # Initialize train datapipes
    train_dataset_mapper = fs.get_mapper(cfg.dataset.train_dataset_filename)
    train_datapipe = SeqZarrDatapipe(
        zarr_dataset=zarr.open(train_dataset_mapper, mode='r'),
        variables=['time', 'predicted', 'unpredicted'],
        batch_size=cfg.training.batch_size,
        num_steps=2,
        shuffle=True,
        device=dist.device,
        process_rank=dist.rank,
        world_size=dist.world_size,
    )

    # Initialize validation datapipes
    val_dataset_mapper = fs.get_mapper(cfg.dataset.val_dataset_filename)
    val_datapipe = SeqZarrDatapipe(
        zarr_dataset=zarr.open(val_dataset_mapper, mode='r'),
        variables=['time', 'predicted', 'unpredicted'],
        batch_size=cfg.validation.batch_size,
        num_steps=cfg.validation.num_steps + cfg.training.nr_input_steps,
        shuffle=False,
        device=dist.device,
        process_rank=dist.rank,
        world_size=dist.world_size,
    )

    # Normalizer (TODO: Maybe wrap this into model)
    predicted_batch_norm = nn.BatchNorm2d(cfg.dataset.nr_predicted_variables, momentum=None, affine=False).to(dist.device)
    unpredicted_batch_norm = nn.BatchNorm2d(cfg.dataset.nr_unpredicted_variables, momentum=None, affine=False).to(dist.device)
    def normalize_variables(variables, batch_norm):
        shape = variables.shape
        variables = variables.flatten(0, 1)
        variables = batch_norm(variables)
        variables = variables.view(shape)
        return variables

    # Unroll network
    def unroll(model, predicted_variables, unpredicted_variables, nr_input_steps, cpu=False):
        # Get number of steps to unroll
        steps = unpredicted_variables.shape[1] - nr_input_steps

        # Create first input
        unpred_i = unpredicted_variables[:, :nr_input_steps]
        pred_i = predicted_variables[:, :nr_input_steps]

        # Unroll
        model_predicted = [] 
        for i in range(steps):
            # Get prediction for the first step
            model_pred_i = model(torch.cat([pred_i, unpred_i], dim=2).flatten(1, 2))
            model_predicted.append(model_pred_i)

            # Create new inputs
            unpred_i = unpredicted_variables[:, i : nr_input_steps + i]
            pred_i = torch.cat([pred_i[:, 1:], model_pred_i.unsqueeze(1)], dim=1)

        # Stack predictions
        model_predicted = torch.stack(model_predicted, dim=1)

        return model_predicted

    # Evaluation forward pass
    @StaticCaptureEvaluateNoGrad(model=model, logger=logger, use_graphs=False)
    def eval_forward(model, predicted_variables, unpredicted_variables, nr_input_steps):
        # Forward pass
        net_predicted_variables = unroll(model, predicted_variables, unpredicted_variables, nr_input_steps)

        # Get l2 loss
        num_elements = torch.prod(torch.Tensor(list(net_predicted_variables.shape[1:])))
        loss = torch.sum(torch.pow(net_predicted_variables - predicted_variables[:, nr_input_steps:], 2)) / num_elements

        return loss, net_predicted_variables, predicted_variables[:, nr_input_steps:]

    # Training forward pass
    @StaticCaptureTraining(model=model, optim=optimizer, logger=logger)
    def train_step_forward(model, predicted_variables, unpredicted_variables, nr_input_steps):
        # Forward pass
        net_predicted_variables = unroll(model, predicted_variables, unpredicted_variables, nr_input_steps)

        # Compute loss
        loss = batch_normalized_mse(net_predicted_variables, predicted_variables[:, nr_input_steps:])

        return loss

    # Main training loop
    for epoch in range(max(1, loaded_epoch + 1), cfg.training.max_epochs + 1):
        # Wrap epoch in launch logger for console / WandB logs
        with LaunchLogger(
            "train", epoch=epoch, num_mini_batch=len(train_datapipe), epoch_alert_freq=10
        ) as log:

            # Track memory throughput
            tic = time.time()
            nr_bytes = 0

            # Training loop
            for j, data in tqdm(enumerate(train_datapipe)):
                # Get predicted and unpredicted variables
                predicted_variables = data[0]["predicted"]
                unpredicted_variables = data[0]["unpredicted"]

                # Normalize variables
                predicted_variables = normalize_variables(predicted_variables, predicted_batch_norm)
                unpredicted_variables = normalize_variables(unpredicted_variables, unpredicted_batch_norm)

                # Log memory throughput
                nr_bytes += predicted_variables.element_size() * predicted_variables.nelement()
                nr_bytes += unpredicted_variables.element_size() * unpredicted_variables.nelement()

                # Perform training step
                loss = train_step_forward(model, predicted_variables, unpredicted_variables, cfg.training.nr_input_steps)
                log.log_minibatch({"loss": loss.detach()})

            # Log learning rate
            log.log_epoch({"Learning Rate": optimizer.param_groups[0]["lr"]})

            # Log memory throughput
            log.log_epoch({"GB/s": nr_bytes / (time.time() - tic) / 1e9})

        # Perform validation
        if dist.rank == 0:

            # Wrap validation in launch logger for console / WandB logs
            with LaunchLogger("valid", epoch=epoch) as log:

                # Switch to eval mode
                if hasattr(model, "module"):
                    model.module.eval()
                else:
                    model.eval()

                # Validation loop
                loss_epoch = 0.0
                num_examples = 0
                for i, data in enumerate(val_datapipe):

                    # Get predicted and unpredicted variables
                    predicted_variables = data[0]["predicted"]
                    unpredicted_variables = data[0]["unpredicted"]

                    # Normalize variables
                    predicted_variables = normalize_variables(predicted_variables, predicted_batch_norm)
                    unpredicted_variables = normalize_variables(unpredicted_variables, unpredicted_batch_norm)

                    # Perform validation step and compute loss
                    loss, net_predicted_variables, predicted_variables = eval_forward(model, predicted_variables, unpredicted_variables, cfg.training.nr_input_steps)
                    loss_epoch += loss.detach().cpu().numpy()
                    num_examples += predicted_variables.shape[0]
            
                    # Plot validation on first batch
                    if i == 0:
                        net_predicted_variables = net_predicted_variables.cpu().numpy()
                        predicted_variables = predicted_variables.cpu().numpy()
                        for chan in range(net_predicted_variables.shape[2]):
                            plt.close("all")
                            fig, ax = plt.subplots(
                                3, net_predicted_variables.shape[1], figsize=(15, net_predicted_variables.shape[0] * 5)
                            )
                            for t in range(net_predicted_variables.shape[1]):
                                ax[0, t].set_title("Network prediction")
                                ax[1, t].set_title("Ground truth")
                                ax[2, t].set_title("Difference")
                                ax[0, t].imshow(net_predicted_variables[0, t, chan])
                                ax[1, t].imshow(predicted_variables[0, t, chan])
                                ax[2, t].imshow(net_predicted_variables[0, t, chan] - predicted_variables[0, t, chan])
            
                            fig.savefig(f"forcast_validation_channel{chan}_epoch{epoch}.png")

                # Log validation loss
                log.log_epoch({"Validation error": loss_epoch / num_examples})

                # Switch back to train mode
                if hasattr(model, "module"):
                    model.module.train()
                else:
                    model.train()

        # Sync after each epoch
        if dist.world_size > 1:
            torch.distributed.barrier()

        # Step scheduler
        scheduler.step()

        # Save checkpoint
        if (epoch % 5 == 0 or epoch == 1) and dist.rank == 0:
            # Use Modulus Launch checkpoint
            save_checkpoint(
                "./checkpoints",
                models=model,
                optimizer=optimizer,
                scheduler=scheduler,
                epoch=epoch,
            )

    # Finish training
    if dist.rank == 0:
        logger.info("Finished training!")


if __name__ == "__main__":
    main()