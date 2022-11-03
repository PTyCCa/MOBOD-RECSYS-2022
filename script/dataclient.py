import argparse
import os
import shutil
import subprocess
import tempfile
import time

import paramiko
from getpass import getpass

from scp import SCPClient

HOST = "mipt-client.atp-fivt.org"


def run_docker(command: str, echo: bool = True):
    if echo:
        print(f" docker > {command}")

    result = subprocess.run(command.split(" "))
    if result.returncode != 0:
        raise ValueError("Failed to execute docker command")


def run_ssh(
    command: str, ssh: paramiko.SSHClient, skippable: bool = False, echo: bool = True
):
    if echo:
        print(f" ssh > {command}")

    _, stdout, stderr = ssh.exec_command(command)
    err = stderr.read().decode("utf-8")
    if err:
        print(err)
        if not skippable:
            raise ValueError("Command failed")

    out = stdout.read().decode("utf-8")
    if out:
        print(out)


def upload_logs_to_hdfs(command_args, passwd):
    target_hdfs_dir = f"/user/{command_args.user}/{command_args.hdfs_dir[0]}"
    print(
        f"## Uploading data from {command_args.log_dir} to {target_hdfs_dir} on behalf of {command_args.user}"
    )

    local_tmp_dir = tempfile.mkdtemp()
    remote_temp_dir = "tmp/" + str(int(time.time()))

    ssh = None
    try:
        run_docker(
            f"docker cp {command_args.recommender}:{command_args.log_dir} {local_tmp_dir}",
            args.echo,
        )

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=HOST, username=command_args.user, password=password, port=22
        )
        scp = SCPClient(ssh.get_transport())

        run_ssh("mkdir -p " + remote_temp_dir, ssh, echo=args.echo)

        files = os.listdir(local_tmp_dir)
        scp.put([f"{local_tmp_dir}/{f}" for f in files], remote_path=remote_temp_dir)

        run_ssh(f"hadoop fs -mkdir -p {target_hdfs_dir}", ssh, echo=args.echo)

        if command_args.cleanup:
            run_ssh(
                f"hadoop fs -rm {target_hdfs_dir}/*",
                ssh,
                skippable=True,
                echo=args.echo,
            )

        run_ssh(
            f"hadoop fs -put {remote_temp_dir}/* {target_hdfs_dir}", ssh, echo=args.echo
        )
    finally:
        shutil.rmtree(local_tmp_dir)

        if ssh is not None:
            run_ssh(f"rm -r {remote_temp_dir}", ssh, echo=args.echo)
            ssh.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--user", help=f"Login used to access {HOST}", type=str, required=True,
    )
    parser.add_argument(
        "--recommender",
        help="Recommender service docker container",
        type=str,
        default="recommender-container",
    )
    parser.add_argument(
        "--echo",
        help="Print command before executing it",
        action="store_true",
        default=True,
    )

    subparsers = parser.add_subparsers()
    log_2_hdfs = subparsers.add_parser(
        "log2hdfs", help="Upload recommender logs to HDFS"
    )
    log_2_hdfs.add_argument(
        "--cleanup", help="clean hdfs dir before", action="store_true", default=False
    )
    log_2_hdfs.add_argument(
        "--log-dir",
        help="Directory containing the uploaded log files",
        type=str,
        default="/app/log/.",
    )
    log_2_hdfs.add_argument(
        "hdfs_dir",
        help="Target HDFS directory (rooted at user's home)",
        type=str,
        nargs=1,
    )
    log_2_hdfs.set_defaults(func=upload_logs_to_hdfs)

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    password = getpass(f"{HOST} password for {args.user}:")
    args.func(args, password)
