from typing import List
import click
from .commands.show import show


@click.group()
def cli():
    pass


cli.add_command(show)


def main(args: List[str] = [], prog_name="spikes-bjarke-analysis"):
    cli(args=args, prog_name=prog_name, standalone_mode=True)
