# Transient Conjugate Heat Transfer (DoMINO)

This example trains and runs a DoMINO model on transient conjugate heat transfer simulations. Raw SimScale dumps (VTU per timestep) are preprocessed into NPZs, then the model is trained to predict surface and volume fields over future timesteps, and inference can write VTKs for inspection.

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

## Key scripts

- `process_data.py` — preprocess raw VTU folders into NPZs (`<processed_dir>/train|val`) and stats. Skips sims already processed.
- `train.py` — trains DoMINO using the processed NPZs and writes checkpoints/tensorboard to `${project.output_root}`.
- `inference.py` — runs a trained checkpoint on raw VTU folders, writing per-timestep surface `.vtp` and volume `.vtu` files with `pred_*` (and optional `gt_*`) fields.

## Quickstart

1) Configure paths in `conf/config.yaml`:
   - `data.raw_dir` → root of raw simulations
   - `data.processed_dir` → where NPZs/stats will be written
   - `project.output_root` → training outputs (checkpoints, tensorboard, inference)

2) Preprocess:
```
python process_data.py
```

3) Train:
```
python train.py
```
Checkpoints land in `${project.output_root}/checkpoints`; tensorboard in `${project.output_root}/tensorboard`.

4) Inference to VTK:
```
python inference.py inference.checkpoint=${project.output_root}/checkpoints inference.output_dir=${project.output_root}/inference
```
Writes per-timestep `surface/*.vtp` and `volume/<region>.vtu` files under `${project.output_root}/inference/<sim_name>/`.

## Notes and tips

- Set `model.model_type: combined` to train volume outputs; `surface` trains only surface fields.
- `train.resume: true/false` controls checkpoint loading; default is resume.
- `train.amp: true` enables autocast + GradScaler on CUDA.
- `process_data.py` uses `preprocess_workers` for parallel preprocessing and skips sims that already have NPZs.
- Normalization expects stats in `<processed_dir>/stats`; missing stats will raise.
