#!/bin/sh

#pull latest commit
git pull

#build virtualenv if it doesn't exist already
if [ ! -d $HOME"/repositories/cliquesadmin/venv/" ]; then
    virtualenv venv
fi

. $HOME/repositories/cliquesadmin/activate

#install all latest requirements
pip install -r requirements.txt

if [ ! -d $HOME"/repositories/cliques-config" ]; then
    git clone git@github.com:cliquesads/smartertravel-config.git ../cliques-config
    ln -s ../cliques-config config
else
    cd ../cliques-config
    git pull
fi