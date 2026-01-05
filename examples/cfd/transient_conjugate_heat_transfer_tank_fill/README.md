# Transient Conjugate Heat Transfer (DoMINO)

This example trains and runs a DoMINO model on transient conjugate heat transfer simulations. Raw CFD solver dumps (VTU per timestep) are preprocessed into NPZs, then the model predicts surface and volume fields for multiple future timesteps in one shot. Inference can write VTKs for inspection.

## Data layout

Raw simulations (per case) are laid out as:

```
<raw_dir>/<sim_name>/<sim_name>/
├─ sim_0/                   # initial timestep (geometry only)
│  ├─ sim_0.boundaries.vtu  # surface mesh (boundary triangles)
│  └─ FLUID0_REG0.vtu, SOLID*.vtu  # volume regions (cell-centered)
├─ sim_1/, sim_2/, ..., sim_N/   # subsequent timesteps with surface/volume fields
└─ probe_points_*.csv, inlet_*.csv, residuals.csv ... (aux files, ignored)
```

`process_data.py` traverses both levels of folders to find `sim_*` timesteps, packs each simulation into a single NPZ, and writes scaling stats to `<processed_dir>/stats`.

## Simulation naming and global parameters

The default preprocessing extracts global parameters from the simulation folder name. It expects a pattern like:

```
<prefix>_<pressure>bar_<temperature>C_<runtime>s
```

The `<prefix>` can include a coil tag (for example, `frontcoil`), which is mapped to `variables.global_parameters.coil_position` in `conf/config.yaml`. If your dataset uses different naming or parameters, update `parse_params` in `process_data.py` and the `variables.global_parameters` section in the config.

## Key scripts

- `process_data.py` — preprocess raw VTU folders into NPZs (`<processed_dir>/train|val`) and stats. Skips sims already processed.
- `train.py` — trains DoMINO using the processed NPZs and writes checkpoints/tensorboard to `${project.output_root}`.
- `inference.py` — runs a trained checkpoint on raw VTU folders, writing per-timestep surface `.vtp` and volume `.vtu` files with `pred_*` (and optional `gt_*`) fields.

## Quickstart

1) Configure paths in `conf/config.yaml`, or pass overrides in the commands below.

2) Preprocess:
```
python process_data.py data.raw_dir=/path/to/raw_simulations data.processed_dir=/path/to/processed_npz
```

3) Train:
```
python train.py data.processed_dir=/path/to/processed_npz project.output_root=/path/to/outputs
```
Checkpoints land in `${project.output_root}/checkpoints`; tensorboard in `${project.output_root}/tensorboard`.

4) Inference to VTK:
```
python inference.py data.raw_dir=/path/to/raw_simulations data.processed_dir=/path/to/processed_npz inference.checkpoint=/path/to/outputs/checkpoints inference.output_dir=/path/to/outputs/inference
```
Writes per-timestep `surface/*.vtp` and `volume/<region>.vtu` files under `${inference.output_dir}/<sim_name>/`.

## Training approach (one-shot forecasting)

- Each sample uses geometry from `sim_0` and concatenates fields from `sim_1..sim_T` into a single target tensor.
- The target is stacked by timestep in the channel dimension, so the model predicts the entire horizon in one forward pass (no autoregressive rollout).
- The number of predicted steps is controlled by `data.future_steps`; smaller simulations are padded and masked so training still works.
- Use `model.model_type: surface | volume | combined` to control which outputs are trained.

## Notes and tips

- `data.val_fraction` or `data.splits.train/val` controls the train/val split.
- `train.resume: true/false` controls checkpoint loading; default is resume.
- `train.amp: true` enables autocast + GradScaler on CUDA.
- Normalization expects stats in `<processed_dir>/stats`; missing stats will raise unless you disable `model.normalization`.
- Inference uses the same stats by default; override with `inference.stats_dir` if needed.
