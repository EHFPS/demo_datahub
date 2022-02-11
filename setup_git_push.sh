#!/bin/bash

mkdir -p -m 700 ~/.ssh
install -m 600 $CI_DEPLOY_PRIVATE_KEY ~/.ssh/id_rsa
ssh-keyscan -t rsa $CI_SERVER_HOST >> ~/.ssh/known_hosts
git config --global user.email "$GITLAB_USER_EMAIL"
git config --global user.name "$GITLAB_USER_LOGIN"
git remote set-url origin git@$CI_SERVER_HOST:$CI_PROJECT_PATH.git
git fetch --all
git reset --hard origin/$CI_COMMIT_BRANCH
git checkout $CI_COMMIT_BRANCH
