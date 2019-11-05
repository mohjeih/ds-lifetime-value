#!/usr/bin/env bash

export TZ=America/Montreal

SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`
CURRENT_DIR=`pwd`

# Activate virtual environment
export PATH="/home/mjeihoonian/miniconda3/bin:$PATH"
source activate cltv_env

SOURCE_CODE=$SCRIPTPATH"/.."
CONFIGURATION_DIR=$SCRIPTPATH"/../../config"
LOG_DIR=$SCRIPTPATH"/../../log"


# Export Python path
export PYTHONPATH=$SOURCE_CODE

export CONFIGURATION_FILE=$CONFIGURATION_DIR"/config.yml"
export GOOGLE_APPLICATION_CREDENTIALS=$CONFIGURATION_DIR"/ssense-3c92053ad127.json"

python3 $SOURCE_CODE"/src/train.py" --last_n_weeks 52 --aws_env ssense-cltv-qa --clf_model clf --reg_model reg $@ &> $LOG_DIR"/train.log"
