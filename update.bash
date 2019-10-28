#!/usr/bin/env bash

update_source_code() {
    GIT_REPO=$1
    echo $GIT_REPO
    cd $GIT_REPO
    git pull &>> $CURRENT_DIR/update.log
    cat $CURRENT_DIR/update.log
}

#update_python_environment() {
#    # Activate virtuale enviromment
#    source activate recommendation_env
#
#    cd $GIT_REPO
#    # update dependency
#    conda  env  update -f=environment.yml
#}
#
#SCRIPT=$(readlink -f $0)
#GIT_REPO=`dirname $SCRIPT`
#CURRENT_DIR=`pwd`
#
#
#> $CURRENT_DIR/update.log
#
update_source_code `dirname $SCRIPT`
#update_python_environment $CURRENT_DIR
#cd