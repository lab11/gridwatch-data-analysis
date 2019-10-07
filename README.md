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

Run the setup script

```
$ ./ipynb-setup.sh
```

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
$ git clone --recursive git@github.com:lab11/gridwatch-data-analsys
```
should work as expected. If you already have the repository you can run:

```
$ git submodule update --init --recursive
```

to get the submodule.
