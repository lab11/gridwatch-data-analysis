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
$ git config filter.clean_ipynb.clean ipynb_drop_output.py
$ git config filter.clean_ipynb.smudge cat
```

Alternatively just run the setup script

```
$ ./ipynb-setup.sh
```

## To use the tools


# To Access Encrypted Data

There is a keybase encrypted data repository that we use internally. It is
placed as a submodule of this repo. To access it you must first be part of the
gridwatch keybase team, then run:

```
$ git config --global --add protocol.keybase.allow always
```

in your terminal to enable git to transport keybase repos as submodules.

If you do both of these things then

```
$ git clone --recursive git@github.com:lab11/powerwatch-site-selection
```
should work as expected. If you already have the repository you can run:

```
$ git submodule update --init --recursive
```

to get the submodule.
