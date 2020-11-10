from typing import List
import click
from .show import show
from .download import download


@click.group()
def cli():
    pass


cli.add_command(show)
cli.add_command(download)


def main(args: List[str] = [], prog_name="dataset"):
    cli(args=args, prog_name=prog_name, standalone_mode=True)
