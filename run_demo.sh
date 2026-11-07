#!/usr/bin/env bash
# Démo hors ligne sur les données synthétiques (aucun accès réseau nécessaire).
# Les sorties vont dans demo_output/ : la base et les rapports réels ne sont pas touchés.
set -e
cd "$(dirname "$0")"
S=data/samples
python src/pipeline.py \
  --raw-file $S/magiceden_mad_lads_SYNTHETIC.json \
  --raw-file $S/magiceden_okay_bears_SYNTHETIC.json \
  --raw-file $S/magiceden_claynosaurz_SYNTHETIC.json \
  --raw-file $S/magiceden_famous_fox_federation_SYNTHETIC.json \
  --fx-file  $S/coingecko_sol_usd_SYNTHETIC.json --output-root demo_output "$@"
