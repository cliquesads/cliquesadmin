#!/bin/sh

#pull latest commit
git pull

#build virtualenv if it doesn't exist already
if [[ ! -d $HOME"/repositories/cliquesadmin/venv/" ]]; then
    virtualenv venv
fi

source $HOME/repositories/cliquesadmin/activate

#install all latest requirements
pip install -r requirements.txt