import argparse
import asyncio
import os
import sys
import json
import torch
import time
import digitalocean
from tqdm import tqdm
from rich import box
from rich.align import Align
from rich.console import Console
from rich.live import Live
from rich.measure import Measurement
from rich.table import Table
from rich.text import Text
from rich.live import Live
from rich.table import Table

from fabric import Connection
import bittensor
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger
logger = logger.opt(colors=True)

import nest_asyncio
nest_asyncio.apply()

TAG = 'zz1'
TOKEN = "3fb6c0ce7a1a75933adbdab09b43745e998dc1a1d713c4873fe52f5e1e883a27"
KEY = '/Users/const/.ssh/kusanagi'

def parse_config():
    parser = argparse.ArgumentParser(description="zz1", usage="zz1 <command> <command args>", add_help=True)
    parser._positionals.title = "commands"
    parser.add_argument (
        '--debug', 
        dest='debug', 
        action='store_true', 
        help='''Set debug'''
    )
    parser.set_defaults ( 
        debug=False 
    )
    command_parsers = parser.add_subparsers( dest='command' )
    status_parser = command_parsers.add_parser('status', help='''Show mining overview''')
    status_parser.add_argument("--names", type=str, required=False, nargs='*', action='store', help="A list of nodes (hostnames) the selected command should operate on")
    status_parser.add_argument (
        '--live', 
        dest='live', 
        action='store_true', 
        help='''Set live table'''
    )
    status_parser.set_defaults ( 
        liva=False 
    )
    bittensor.wallet.Wallet.add_args( status_parser )
    bittensor.subtensor.Subtensor.add_args( status_parser )
    bittensor.dendrite.Dendrite.add_args( status_parser )

    create_parser = command_parsers.add_parser('create', help='''create''')
    create_parser.add_argument('--name', dest="name", type=str, required=True)
    
    wallet_parser = command_parsers.add_parser('wallet', help='''wallet''')
    wallet_parser.add_argument("--names", type=str, nargs='*', required=True, action='store', help="A list of nodes (hostnames) the selected command should operate on")
    wallet_parser.add_argument('--coldkey', dest="coldkey", type=str, required=True, help='Coldkey name to load hotkeys from. All hotkeys should exist in this coldkey account.')
    wallet_parser.add_argument("--hotkeys", type=str, nargs='*', required=False, action='store', help="A list of hotkeys to load into wallet, should align with passed names")

    checkout_parser = command_parsers.add_parser('checkout', help='''configure''')
    checkout_parser.add_argument("--names", type=str, nargs='*', required=True, action='store', help="A list of nodes (hostnames) the selected command should operate on")
    checkout_parser.add_argument('--branch', dest="branch", type=str, required=True)

    install_parser = command_parsers.add_parser('install', help='''install''')
    install_parser.add_argument("--names", type=str, nargs='*', required=True, action='store', help="A list of nodes (hostnames) the selected command should operate on")

    start_parser = command_parsers.add_parser('start', help='''start''')
    start_parser.add_argument("--names", type=str, nargs='*', required=True, action='store', help="A list of nodes (hostnames) the selected command should operate on")
    start_parser.add_argument('--miner', dest="miner", default='gpt2_genesis', type=str, required=False)

    stop_parser = command_parsers.add_parser('stop', help='''stop''')
    stop_parser.add_argument("--names", type=str, nargs='*', required=True, action='store', help="A list of nodes (hostnames) the selected command should operate on")

    logs_parser = command_parsers.add_parser('logs', help='''logs''')
    logs_parser.add_argument("--names", type=str, nargs='*', required=True, action='store', help="A list of nodes (hostnames) the selected command should operate on")
    logs_parser.add_argument('--miner', dest="miner", default='gpt2_genesis', type=str, required=False)
    

    args = parser.parse_args()
    config = bittensor.config.Config.to_config(parser); 
    return config

def connection_for_droplet( droplet ) -> Connection:
    con = Connection( droplet.ip_address, user='root', connect_kwargs={
            "key_filename" : KEY
        })
    return con

def can_connect( connection ) -> bool:
    try:
        result = connection.run('')
        return True
    except:
        return False

def droplet_with_name( name: str ):
    manager = digitalocean.Manager( token = TOKEN )
    droplets = manager.get_all_droplets( tag_name = [ TAG ])
    for droplet in droplets:
        if droplet.name == name:
            return droplet
    return None

def make_bittensor_dir( connection ):
    make_bittensor_dir = 'mkdir -p ~/.bittensor/bittensor/'
    logger.debug("Making bittensor director: {}", make_bittensor_dir)
    make_bittensor_dir_result = connection.run(make_bittensor_dir, hide=True)
    logger.debug(make_bittensor_dir_result)
    return make_bittensor_dir_result

def remove_bittensor_installation( connection ):
    remove_bittensor_command = 'rm -rf ~/.bittensor/bittensor'
    logger.debug("Removing bittensor installation: {}", remove_bittensor_command)
    remove_result = connection.run(remove_bittensor_command, hide=True)
    logger.debug(remove_result)
    return remove_result

def git_clone_bittensor( connection ):
    clone_bittensor = "git clone https://github.com/opentensor/bittensor.git ~/.bittensor/bittensor"
    logger.debug("Pulling bittensor from github: {}", clone_bittensor)
    clone_result = connection.run(clone_bittensor, hide=True)
    logger.debug(clone_result)
    return clone_result

def git_checkout_bittensor( connection, branch ):
    if "tags/" in branch:
        branch_str = "%s -b tag-%s" % (branch, branch.split("/")[1])
    else:
        branch_str = branch
    checkout_command = 'cd ~/.bittensor/bittensor ; git checkout %s' % branch_str
    logger.debug("Checking out branch: {}", checkout_command)
    checkout_result = connection.run(checkout_command, hide=True, warn=True)
    logger.debug(checkout_result)
    return checkout_result

def git_branch_bittensor( connection ) -> str:
    get_branch_command = 'cd ~/.bittensor/bittensor ; git branch --show-current'
    logger.debug("Determining installed branch: {}", get_branch_command)
    get_branch_result = connection.run(get_branch_command, hide=True, warn=True)
    logger.debug(get_branch_result)
    return get_branch_result

def make_wallet_dirs( connection ):
    mkdirs_command = 'mkdir -p /root/.bittensor/wallets/default/hotkeys'
    logger.debug("Making wallet dirs: {}", mkdirs_command)
    mkdir_result = connection.run( mkdirs_command, warn=True, hide=True )
    logger.debug(mkdir_result)
    return mkdir_result

def copy_hotkey( connection, wallet ):
    hotkey_str = open(wallet.hotkeyfile, 'r').read()
    copy_hotkey_command = "echo '%s' > /root/.bittensor/wallets/default/hotkeys/default" % hotkey_str
    logger.debug("Copying hotkey: {}", copy_hotkey_command)
    copy_hotkey_result = connection.run( copy_hotkey_command, warn=True, hide=True )
    logger.debug(copy_hotkey_result)
    return copy_hotkey_result

def copy_coldkeypub( connection, wallet ):
    coldkeypub_str = open(wallet.coldkeypubfile, 'r').read()
    copy_coldkeypub_command = "echo '%s' > /root/.bittensor/wallets/default/coldkeypub.txt" % coldkeypub_str
    logger.debug("Copying coldkeypub: {}", copy_coldkeypub_command)
    copy_coldkey_result = connection.run( copy_coldkeypub_command, warn=True, hide=True )
    logger.debug(copy_coldkey_result)
    return copy_coldkey_result

def install_python_deps( connection ):
    install_python_deps_command = "sudo apt-get update && sudo apt-get install --no-install-recommends --no-install-suggests -y apt-utils curl git cmake build-essential "
    logger.debug("Installing python deps: {}", install_python_deps_command)
    install_python_deps_result = connection.run(install_python_deps_command, hide=True, warn=True)
    logger.debug(install_python_deps_result)
    return install_python_deps_result

def install_python( connection ):
    install_python_command = "sudo apt-get install --no-install-recommends --no-install-suggests -y python3.8"
    logger.debug("Installing python: {}", install_python_command)
    install_python_result = connection.run(install_python_command, hide=True, warn=True)
    logger.debug(install_python_result)
    return install_python_result

def install_bittensor_deps( connection ):
    install_bittensor_deps_command = "sudo apt-get install --no-install-recommends --no-install-suggests -y python3-pip python3.8-dev python3.8-venv"
    logger.debug("Installing bittensor deps: {}", install_bittensor_deps_command)
    install_bittensor_deps_result = connection.run(install_bittensor_deps_command, hide=True, warn=True)
    logger.debug(install_bittensor_deps_result)
    return install_bittensor_deps_result

def install_swapspace( connection ):
    install_swap_command = "sudo fallocate -l 8G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile && sudo cp /etc/fstab /etc/fstab.bak"
    logger.debug("Installing swapspace: {}", install_swap_command)
    install_swap_result = connection.run(install_swap_command, hide=True, warn=True)
    logger.debug(install_swap_result)
    return install_swap_result

def install_bittensor( connection ):
    install_command = "cd ~/.bittensor/bittensor ; pip3 install -e ."
    logger.debug("Installing bittensor: {}", install_command)
    install_result = connection.run(install_command, hide=True, warn=True)
    logger.debug(install_result)
    return install_result

def is_installed( connection ) -> bool:
    check_bittensor_install_command = 'python3 -c "import bittensor"'
    logger.debug("Checking installation: {}", check_bittensor_install_command)
    check_install_result = connection.run(check_bittensor_install_command, hide=True, warn=True)
    if check_install_result.failed:
        return False
    else:
        return True

def get_hotkey( connection ) -> str:
    cat_hotkey_command = "cat /root/.bittensor/wallets/default/hotkeys/default"
    logger.debug("Getting hotkey: {}", cat_hotkey_command)
    cat_hotkey_result = connection.run(cat_hotkey_command, warn=True, hide=True)
    if cat_hotkey_result.failed:
        return None
    hotkey_info = json.loads(cat_hotkey_result.stdout)
    return hotkey_info['publicKey']

def get_coldkeypub( connection ) -> str:
    cat_coldkey_command = "cat /root/.bittensor/wallets/default/coldkeypub.txt"
    logger.debug("Getting coldkey: {}", cat_coldkey_command)
    cat_coldkey_result = connection.run(cat_coldkey_command, warn=True, hide=True)
    if cat_coldkey_result.failed:
        return None
    return cat_coldkey_result.stdout.strip()

def get_branch( connection ) -> str:
    branch_result = git_branch_bittensor( connection )
    if branch_result.failed:
        return None
    branch_name = branch_result.stdout.strip()
    return branch_name

def start_miner( connection, miner_name ):
    start_miner_command = "cd ~/.bittensor/bittensor ; nohup python3 miners/{}.py --miner.epoch_length 20 &> /dev/null &".format( miner_name )
    logger.debug("Starting miner: {}", start_miner_command)
    start_miner_result = connection.run(start_miner_command, warn=True, hide=True, pty=False)
    logger.debug( start_miner_result )
    return start_miner_result

def stop_miner( connection ):
    stop_miner_command = "pgrep -f miners | xargs kill"
    logger.debug("Stopping miner: {}", stop_miner_command)
    stop_miner_result = connection.run(stop_miner_command, warn=True, hide=True)
    logger.debug( stop_miner_result )
    return stop_miner_result

def is_miner_running( connection ) -> bool:
    miner_running_command = 'pgrep -lf miners'
    logger.debug("Getting miner status: {}", miner_running_command)
    miner_running_result = connection.run(miner_running_command, hide=True, warn=True)
    if miner_running_result.failed:
        if miner_running_result.exited == 1:
            return False
        else:
            logger.warning("An undefined error occured while determining if bittensor is running : {}", miner_running_result)
    command_output = miner_running_result.stdout
    if command_output:
        return True
    else:
        return False

def get_logs( connection, miner ) -> str:
    get_logs_command = "tail -n 50 ~/.bittensor/miners/default-default/{}/bittensor_output.log".format(miner)
    logger.debug("Getting logs miner: {}", get_logs_command)
    get_logs_result = connection.run(get_logs_command, warn=True, hide=True)
    logger.debug( get_logs_result )
    if get_logs_result.failed:
        return None
    return get_logs_result.stdout

def is_miner_subscribed( subtensor, ip, hotkey ) -> bool:
    logger.debug("Getting subscription status for {},{}", ip, hotkey)
    uid = subtensor.get_uid_for_pubkey( hotkey )
    if uid != None:
        neuron = subtensor.get_neuron_for_uid( uid )
        if neuron['ip'] == bittensor.utils.networking.ip_to_int(ip):
            return True
        else:
            return False
    else:
        return False

def create_droplet( manager, name ):
    manager = digitalocean.Manager(token=TOKEN)
    keys = manager.get_all_sshkeys()
    droplet = digitalocean.Droplet(
        token=TOKEN,
        tag=TAG,
        name=name,
        size_slug='s-1vcpu-1gb',
        ssh_keys=keys,
        backups=False
    )
    droplet.create()
    return droplet

def get_droplet_status( droplet ) -> str:
    status = "booting" if droplet.status == "new" else droplet.status
    return status

def create( confg ):
    # Create manager with token.
    manager = digitalocean.Manager(token=TOKEN)

    # Create droplet
    droplet = create_droplet( manager = manager, name = config.name )
    logger.success( 'droplet status: {}', get_droplet_status(droplet) )

def get_logs_for_droplet_with_name( args ):
    try:
        name = args[0]
        config = args[1] 

        # Find droplet.
        droplet = droplet_with_name( name )
        if droplet == None:
            logger.error('<blue>{}</blue>: Not found.', name)
            return
        logger.success('<blue>{}</blue>: Found.', name)

        # Make connection.
        connection = connection_for_droplet( droplet )
        if not can_connect( connection ):
            logger.error('<blue>{}</blue>: Failed to make connection to droplet', name)
            return
        logger.success('<blue>{}</blue>: Made connection to droplet', name)

        # Get logs
        logs = get_logs(connection, config.miner )
        if logs == None:
            logger.error('<blue>{}</blue>: Failed to pull logs', name)
        else:
            logger.info('<blue>{}</blue>: Logs: {}', name, logs)
        logger.success('<blue>{}</blue>: DONE', name)
    except Exception as e:
        logger.exception( e )

def stop_droplet_with_name( args ):
    try:
        name = args[0]
        config = args[1] 

        # Find droplet.
        droplet = droplet_with_name( name )
        if droplet == None:
            logger.error('<blue>{}</blue>: Not found.', name)
            return
        logger.success('<blue>{}</blue>: Found.', name)

        # Make connection.
        connection = connection_for_droplet( droplet )
        if not can_connect( connection ):
            logger.error('<blue>{}</blue>: Failed to make connection to droplet', name)
            return
        logger.success('<blue>{}</blue>: Made connection to droplet', name)

        # Stop miner
        if stop_miner(connection).failed:
            pass
        else:
            logger.success('<blue>{}</blue>: Stopped miner.', name)

        # Check miner status.
        if is_miner_running( connection ):
            logger.error('<blue>{}</blue>: Failed to stop miner from running', name)
            return
        logger.success('<blue>{}</blue>: Miner is stopped.', name)
        logger.success('<blue>{}</blue>: DONE', name)
    except Exception as e:
        logger.exception( e )

def start_droplet_with_name( args ):
    try:
        name = args[0]
        config = args[1] 

        # Find droplet.
        droplet = droplet_with_name( name )
        if droplet == None:
            logger.error('<blue>{}</blue>: Not found.', name)
            return
        logger.success('<blue>{}</blue>: Found.', name)

        # Make connection.
        connection = connection_for_droplet( droplet )
        if not can_connect( connection ):
            logger.error('<blue>{}</blue>: Failed to make connection to droplet', name)
            return
        logger.success('<blue>{}</blue>: Made connection to droplet', name)

        # Stop miner
        if stop_miner(connection).failed:
            pass
        else:
            logger.success('<blue>{}</blue>: Stopped miner.', name)

        # Start miner
        if start_miner( connection, config.miner ).failed:
            logger.error('Failed to start miner', name)
            return
        logger.success('<blue>{}</blue>: Started miner', name)

        # Check miner status.
        if not is_miner_running( connection ):
            logger.error('<blue>{}</blue>: Miner is not running', name)
            return
        logger.success('<blue>{}</blue>: Miner is running', name)
        logger.success('<blue>{}</blue>: DONE', name)
    except Exception as e:
        logger.exception( e )


def install_bittensor_on_droplet_with_name( args ):
    try:
        name = args[0]
        config = args[1]    

        # Find droplet.
        droplet = droplet_with_name( name )
        if droplet == None:
            logger.error('<blue>{}</blue>: Not found.', name)
            return
        logger.success('<blue>{}</blue>: Found.', name)

        # Make connection.
        connection = connection_for_droplet( droplet )
        if not can_connect( connection ):
            logger.error('<blue>{}</blue>: Failed to make connection to droplet', name)
            return
        logger.success('<blue>{}</blue>: Made connection to droplet', name)

        # Install swapspace
        if install_swapspace(connection).failed:
            logger.success('<blue>{}</blue>: Swapspace already installed', name)
        logger.success('<blue>{}</blue>: Installed swapspace.', name)

        # Install python deps.
        if install_python_deps( connection ).failed:
            logger.error('<blue>{}</blue>: Failed to install python deps', name)
            return
        logger.success('<blue>{}</blue>: Python deps installation successful.', name)

        # Install python.
        if install_python( connection ).failed:
            logger.error('<blue>{}</blue>: Failed to install python', name)
            return
        logger.success('<blue>{}</blue>: Python installation successful.', name)

        # install bittensor python deps.
        if install_bittensor_deps(connection).failed:
            logger.error('<blue>{}</blue>: Failed to install bittensor deps', name)
            return
        logger.success('<blue>{}</blue>: Bittensor deps installation successful.', name)

        # Install bittensor.
        if install_bittensor(connection).failed:
            logger.error('<blue>{}</blue>: Failed to install bittensor', name)
            return
        logger.success('<blue>{}</blue>: Bittensor installation successful.', name)

        # Check installed
        if not is_installed( connection ):
            logger.error('<blue>{}</blue>: Bittensor is not installed', name)
            return
        logger.success('<blue>{}</blue>: Bittensor is installed.', name)
        logger.success('<blue>{}</blue>: DONE', name)
    except Exception as e:
        logger.exception( e )


def checkout_bittensor_on_droplet_with_name( args ):
    try:
        name = args[0]
        config = args[1] 

        # Find droplet.
        droplet = droplet_with_name( name )
        if droplet == None:
            logger.error('<blue>{}</blue>: Not found.', name)
            return
        logger.success('<blue>{}</blue>: Found.', name)

        # Make connection.
        connection = connection_for_droplet( droplet )
        if not can_connect( connection ):
            logger.error('<blue>{}</blue>: Failed to make connection to droplet', name)
            return
        logger.success('<blue>{}</blue>: Made connection to droplet', name)

        # Make dirs
        if make_bittensor_dir( connection ).failed:
            logger.error('<blue>{}</blue>: Failed to make bittensor dirs.', name)
            return
        else:
            logger.success('<blue>{}</blue>: Made bittensor dirs.', name)

        # Remove bittensor
        if remove_bittensor_installation( connection ).failed:
            logger.error('<blue>{}</blue>: Failed to remove previous bittensor installation', name)
            return
        else:
            logger.success('<blue>{}</blue>: Remove previous bittensor installation', name)

        # Clone bittensor
        if git_clone_bittensor( connection ).failed:
            logger.error('<blue>{}</blue>: Failed to clone bittensor', name)
            return
        else:
            logger.success('<blue>{}</blue>: Cloned bittensor', name)

        # Checkout branch
        if git_checkout_bittensor( connection, config.branch ).failed:
            logger.error('<blue>{}</blue>: Failed to checkout bittensor branch: {}', name, config.branch)
            return
        else:
            logger.success('<blue>{}</blue>: Checked out bittensor branch', name)

        # Get branch
        branch_result = git_branch_bittensor( connection )
        if branch_result.failed:
            logger.error("{}: Failed to get branch", name)
            return
        branch_name = branch_result.stdout.strip()
        if branch_name != config.branch:
            logger.error('<blue>{}</blue>: Failed to properly set branch, branch is {}', name, branch_name)
            return
        else:
            logger.success('<blue>{}</blue>: Branch set to: {}', name, branch_name)
        logger.success('<blue>{}</blue>: DONE', name)
    except Exception as e:
        logger.exception( e )


def laod_wallet_for_droplet( args ):
    try:
        name = args[0]
        config = args[1]

        # Find droplet.
        droplet = droplet_with_name( name )
        if droplet == None:
            logger.error('<blue>{}</blue>: Not found.', name)
            return
        logger.success('<blue>{}</blue>: Found.', name)
        
        # Make connection.
        connection = connection_for_droplet( droplet )
        if not can_connect( connection ):
            logger.error('<blue>{}</blue>: Failed to make connection to droplet')
            return
        logger.success('<blue>{}</blue>: Made connection to droplet', name)

        # Configure wallet.
        wallet = bittensor.wallet.Wallet( 
            name = config.coldkey,
            hotkey = name
        )
        if not wallet.has_hotkey:
            logger.error('<blue>{}</blue>: Wallet does not have hotkey: {}', name, wallet.hotkeyfile)
            wallet.create_new_hotkey(use_password=False)
            logger.error('<blue>{}</blue>: Created new hotkey with name: {}', name, name)
        logger.success('<blue>{}</blue>: Found hotkey: {}', name, name)

        if not wallet.has_coldkeypub:
            logger.error('<blue>{}</blue>: Wallet does not have coldkeypub: {}', name, wallet.coldkeypubfile)
            return
        logger.success('<blue>{}</blue>: Found coldkeypub: {}', name, wallet.coldkeypubfile)
        
        # Make wallet dirs.
        if make_wallet_dirs( connection ).failed:
            logger.error('<blue>{}</blue>: Error creating wallet dirs with command: {}', name, mkdirs_command)
            return
        logger.success('<blue>{}</blue>: Created wallet directories', name)

        # Copy hotkey.
        if copy_hotkey( connection, wallet ).failed:
            logger.error('<blue>{}</blue>: Error copy hotkey with command: {}', name, copy_hotkey_command)
            return
        logger.success('<blue>{}</blue>: Copied hotkey to dir: {}', name, '/root/.bittensor/wallets/default/hotkeys/default')

        # Copy coldkeypub
        if copy_coldkeypub(connection, wallet).failed:
            logger.error('<blue>{}</blue>: Error copy coldkey with command: {}', name, copy_coldkeypub_command)
            return
        logger.success('<blue>{}</blue>: Copied coldkey to dir: {}', name, '/root/.bittensor/wallets/default/coldkeypub.txt')

        # Get hotkey
        hotkey = get_hotkey( connection )
        if hotkey == None:
            logger.error('<blue>{}</blue>: Failed to retrieve hotkey from {}', name, connection.host)
        else:
            logger.success('<blue>{}</blue>: Could retrieve hotkey: {}', name, hotkey)

        # Get coldkeypub
        coldkeypub = get_coldkeypub( connection )
        if coldkeypub == None:
            logger.error('<blue>{}</blue>: Failed to retrieve coldkeypub from {}', name, connection.host)
        else:
            logger.success('<blue>{}</blue>: Could retrieve coldkeypub: {}', name, coldkeypub)

        logger.success('<blue>{}</blue>: DONE', name)
    except Exception as e:
        logger.exception( e )

def install( config ):
    iterables = [ (name, config) for name in config.names]
    with ThreadPoolExecutor(max_workers=10) as executor:
        tqdm(executor.map(install_bittensor_on_droplet_with_name, iterables), total=len(iterables))

def checkout( config ):
    iterables = [ (name, config) for name in config.names]
    with ThreadPoolExecutor(max_workers=10) as executor:
        tqdm(executor.map(checkout_bittensor_on_droplet_with_name, iterables), total=len(iterables))

def start( config ):
    iterables = [ (name, config) for name in config.names]
    with ThreadPoolExecutor(max_workers=10) as executor:
        tqdm(executor.map(start_droplet_with_name, iterables), total=len(iterables))

def stop( config ):
    iterables = [ (name, config) for name in config.names]
    with ThreadPoolExecutor(max_workers=10) as executor:
        tqdm(executor.map(stop_droplet_with_name, iterables), total=len(iterables))

def logs( config ):
    iterables = [ (name, config) for name in config.names]
    with ThreadPoolExecutor(max_workers=10) as executor:
        tqdm(executor.map(get_logs_for_droplet_with_name, iterables), total=len(iterables))

def wallet (config):
    iterables = [ (name, config) for name in config.names]
    with ThreadPoolExecutor(max_workers=10) as executor:
        tqdm(executor.map(laod_wallet_for_droplet, iterables), total=len(iterables))


def status( config ):

    # Globals
    manager = digitalocean.Manager( token = TOKEN )
    wallet =  bittensor.wallet.Wallet( config )
    subtensor = bittensor.subtensor.Subtensor( config )
    meta = bittensor.metagraph.Metagraph()
    dendrite = bittensor.dendrite.Dendrite( config )
    config = config
        
    # Create table.
    def generate_table():

        nonlocal config
        nonlocal manager
        nonlocal subtensor
        nonlocal dendrite
        nonlocal meta

        total_stake = 0.0
        total_incentive = 0.0
        total_rank = 0.0
        total_success = 0
        total_time = 0.0

        # Fill row.
        def get_row( droplet, ):

            nonlocal total_stake
            nonlocal total_incentive
            nonlocal total_rank
            nonlocal total_time
            nonlocal total_success
            nonlocal config

            if droplet.name not in config.names:
                return

            # Setup asyncio loop.
            connection = connection_for_droplet( droplet )

            # Get connection string
            connect_str = '[bold green] YES' if can_connect( connection ) else '[bold red] NO'

            # get hotkey
            hotkey = get_hotkey( connection )
            hotkey_str = hotkey if hotkey != None else '[yellow] None'

            # get coldkey
            coldkeypub = get_coldkeypub( connection )
            coldkeypub_str = coldkeypub if coldkeypub != None else '[yellow] None'

            # get branch 
            branch = get_branch( connection )
            branch_str = branch if branch != None else '[yellow] None'

            # get install status
            if branch != None:
                installed = is_installed( connection )
                is_installed_str =  '[bold green] Yes' if installed else '[bold red] No'
            else:
                installed = False
                is_installed_str = '[bold red] No'

            # get miner status
            if installed:
                is_running = is_miner_running( connection )
                is_running_str =  '[bold green] Yes' if is_running else '[bold red] No'
            else:
                is_running = False
                is_running_str = '[bold red] No'

            # get is_subscribed 
            try:
                uid = meta.hotkeys.index( hotkey )
                is_subscribed = True
                is_subscribed_str =  '[bold green] Yes'
            except:
                is_subscribed = False
                is_subscribed_str = '[bold red] No'

            # get subscription status.
            if is_subscribed:
                stake = meta.S[ uid ].item()
                rank = meta.R[ uid ].item()
                incentive = meta.I[ uid ].item()
                lastemit = int(meta.block - meta.lastemit[ uid ])
                lastemit = "[bold green]" + str(lastemit) if lastemit < 3000 else "[bold red]" + str(lastemit)
                address = str(meta.addresses[uid])
                neuron = meta.neuron_endpoints[ uid ]
                
                total_stake += stake
                total_rank += rank
                total_incentive += incentive * 14400

                uid_str = str(uid)
                stake_str = '[green]\u03C4{:.5}'.format(stake)
                rank_str = '[green]\u03C4{:.5}'.format(rank)
                incentive_str = '[green]\u03C4{:.5}'.format(incentive * 14400)
                lastemit_str = str(lastemit)
                address_str = str(address)

            else:
                uid_str = '[dim yellow] None'
                stake_str = '[dim yellow] None'
                rank_str = '[dim yellow] None'
                incentive_str = '[dim yellow] None'
                lastemit_str = '[dim yellow] None'
                address_str = '[dim yellow] None'


            # Make query and get response.
            if installed and is_running and is_subscribed and wallet.has_hotkey and neuron != None:
                start_time = time.time()
                result, code = dendrite.forward_text( neurons = [neuron], x = [torch.zeros((1,1), dtype=torch.int64)] )
                end_time = time.time()
                code_to_string = bittensor.utils.codes.code_to_string(code.item())
                code_color =  bittensor.utils.codes.code_to_color(code.item()) 
                code_str =  '[' + str(code_color) + ']' + code_to_string 
                query_time_str = '[' + str(code_color) + ']' + "" + '{:.3}'.format(end_time - start_time) + "s"

                if code.item() == 0:
                    total_success += 1
                    total_time += end_time - start_time
            else:
                code_str = '[dim yellow] N/A'
                query_time_str = '[dim yellow] N/A'

            row = [ str(droplet.name), str(droplet.ip_address), str(droplet.region['name']), str(droplet.size_slug), str(connect_str), branch_str, is_installed_str, is_running_str, is_subscribed_str, address_str, uid_str, stake_str, rank_str, incentive_str, lastemit_str, query_time_str, code_str, hotkey_str, coldkeypub_str]
            return row

        # Get latest droplets.
        droplets = manager.get_all_droplets( tag_name = [ TAG ])
        if config.names == None:
            config.names = [droplet.name for droplet in droplets]

        subtensor.connect()
        meta.load()
        meta.sync(subtensor = subtensor, force = False)
        meta.save()
    
        TABLE_DATA = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            if config.live:
                TABLE_DATA = list(executor.map(get_row, droplets))
            else:
                TABLE_DATA = list(tqdm(executor.map(get_row, droplets), total=len(droplets)))
        TABLE_DATA = [row for row in TABLE_DATA if row != None ]
        TABLE_DATA.sort(key = lambda TABLE_DATA: TABLE_DATA[0])

        total_stake_str = '\u03C4{:.7}'.format(total_stake)
        total_rank_str = '\u03C4{:.7}'.format(total_rank)
        total_incentive_str = '\u03C4{:.7}'.format(total_incentive)
        total_time_str = '{:.3}s'.format(total_time / len(config.names)) if total_time != 0 else '0.0s'
        total_success_str = '[bold green]' + str(total_success) + '/[bold red]' +  str( len(config.names) - total_success )

        console = Console()
        table = Table(show_footer=False)
        table_centered = Align.center(table)
        table.title = (
            "[bold white]Miners" 
        )
        table.add_column("[overline white]NAME",  str(len(config.names)), footer_style = "overline white", style='white')
        table.add_column("[overline white]IP", style='blue')
        table.add_column("[overline white]LOC", style='yellow')
        table.add_column("[overline white]SIZE", style='green')
        table.add_column("[overline white]CONN", style='green')
        table.add_column("[overline white]BRNCH", style='bold purple')
        table.add_column("[overline white]INSTL")
        table.add_column("[overline white]RNG")
        table.add_column("[overline white]SUBD")
        table.add_column("[overline white]ADDR", style='blue')
        table.add_column("[overline white]UID", style='yellow')
        table.add_column("[overline white]STAKE(\u03C4)", total_stake_str, footer_style = "overline white", justify='right', style='green', no_wrap=True)
        table.add_column("[overline white]RANK(\u03C4)", total_rank_str, footer_style = "overline white", justify='right', style='green', no_wrap=True)
        table.add_column("[overline white]INCN(\u03C4/d)", total_incentive_str, footer_style = "overline white", justify='right', style='green', no_wrap=True)
        table.add_column("[overline white]LEmit", justify='right', no_wrap=True)
        table.add_column("[overline white]Qry(sec)", total_time_str, footer_style = "overline white", justify='right', no_wrap=True)
        table.add_column("[overline white]Qry(code)", total_success_str, footer_style = "overline white", justify='right', no_wrap=True)
        table.add_column("[overline white]HOT", style='bold blue', no_wrap=False)
        table.add_column("[overline white]COLD", style='blue', no_wrap=False)
        table.show_footer = True

        console.clear()
        for row in TABLE_DATA:
            table.add_row(*row)
        # table.box = None
        table.pad_edge = False
        table.width = None
        return table

    if config.live:
        with Live(generate_table(), refresh_per_second=4) as live:
            while True:
                time.sleep(20)
                table = generate_table()
                live.update(generate_table())
    else:
        table = table = generate_table()
        console = Console()
        console.print(table)

def configure_logging( config ):
    logger.remove()
    if config.debug == True:
        logger.add(sys.stderr, level="TRACE")
    else:
        logger.add(sys.stderr, level="INFO")

def main( config ):
    print(bittensor.config.Config.toString( config ))
    configure_logging( config )
    if config.command == 'status':
        status( config )
    elif config.command == 'create':
        create( config )
    elif config.command == 'wallet':
        wallet( config )
    elif config.command == 'checkout':
        checkout( config )
    elif config.command == 'install':
        install( config )
    elif config.command == 'start':
        start( config )
    elif config.command == 'stop':
        stop( config )
    elif config.command == 'logs':
        logs( config )

if __name__ == "__main__":
    config = parse_config()
    main( config )