# 3rd party indexer task integration

This document describes what is required to integrate in this indexer tasks from
other projects (we will refer to them as 3rd party projects (3pp)). In 
particular, we will refer to Rewards-Agent.

The main guideline for this purpose is that nothing special is required, but 
just to include dependencies and provide requirements for all projects included.

We chose to use this repository as a base for the indexer. In that order we 
assume this project is installed in the current directory, its dependencies 
already installed and the configuration available in a config.json like file.


## Installation and Setup

### Configuration

Using an already working 3pp `config.json` configuration file, read its 
content and merge them into the desired existing indexer configuration.

In our case, Rewards' configuration the settings required probably missing 
in the existing indexer config. is the agent:

{
 'networks': [
        '-network-': {
            'agent' { .. }
        }
   ]
}

 where _<network>_ is the chosen network, and includes the agent address and 
 its key.

And it is also required the MocToken be set inside 
`networks/-network-/addresses`.

### Dependencies and Code setup

The code related to the 3pp must be available for usage in the system. This 
can be achieved in several ways. 

For projects which can be globally installed which make them available to 
other projects, just install them globally (perhaps using their `setup.py` 
as you would do to install any project: `python setup.py install`).

If the 3pp isn't ready for system installation a two-step setup is required:

1. As usual, install dependencies with: 

`python -m pip install --user -r requirements.txt`

using requirements.txt from the 3pp in our case, Rewards.

2. Make code available

Required 3pp project code is inside the `agent/` directory from the 3pp, you 
just copy this directory inside the indexer directory, and it will be 
available to use.


## Execution

Remember that before executing the 3pp module, you probably would like to 
run its setup, like in Rewards project, you better run `set_initial` block 
scripts, and others which would affect settings inside the database.


If you already run indexer this way:

`python taskrunner.py -n network -c config jobs:*`

You should now include the 3pp jobs in the execution, this way:

`python taskrunner.py -n network -c config jobs:* agent.jobs:*`

All jobs should be presented before execution.

Of course in case of having any problem, you can selectively choose what to 
execute, for instance, to know if the reward agent is correctly setup, you 
may find useful to run it alone, this way:

 `python taskrunner.py -n network -c config agent.jobs:*`


----




