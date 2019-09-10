GridWatch Data Analysis
=======================

A storage repo for the tooling and data analysis scripts for GridWatch.

Currently we have processing scripts to support running spark programs, but they
don't allow for you to run ipynb's (which sucks because the checkpointing is nice).

Also committing ipynbs with sensitive data is just difficult.

The solution to this is twofold:

1) Add support to connect ipynb to google cloud dataproc clusters for faster processing

2) Add git tooling to make sure the checkpointed data isn't accidentally committed

## Before you start

You need to setup git to automatically clear ipynb outputs before committing them.

1) Add the ipython cleaner to your path

```
$ echo 'export PATH="$PATH:'$(pwd)/ipython-cleaner'"' >> ~/.bashrc
$ source ~/.bashrc
```

2) Setup git to reference the cleaning script

```
$ echo '*.ipynb  filter=clean_ipynb' >> ~/.config/git/attributes
$ git config --global filter.clean_ipynb.clean ipynb_drop_output.py
$ git config --global filter.clean_ipynb.smudge cat
```

Alternatively just run the setup script

```
$ ./ipynb-setup.sh
```

## To use the tools


