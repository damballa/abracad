#! /bin/sh -e

git checkout master
lein doc
git checkout gh-pages
sleep 1
find doc -type f -exec touch {} +
rsync -r doc/ .
git add .
git commit
git push -u damballa gh-pages
git checkout master
