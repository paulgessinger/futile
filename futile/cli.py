import click
from halo import Halo
import os
import yaml
import sh
import logging
import tempfile
import sys


APP_NAME = "futile"

borg = sh.Command("borg")

logger = logging.getLogger(APP_NAME)


@click.group(invoke_without_command=True)
@click.option("-v", "--verbose", count=True)
@click.pass_context
def main(ctx, verbose):
    global_level = logging.WARNING
    if verbose == 0:
        level = logging.WARNING
    elif verbose == 1:
        level = logging.INFO
        global_level = logging.INFO
    elif verbose == 2:
        level = logging.DEBUG
    else:
        level = logging.DEBUG
        global_level = logging.DEBUG
    logging.basicConfig(
        format="%(levelname)s %(name)s %(filename)s:%(funcName)s %(message)s",
        level=global_level,
    )

    logger.setLevel(level)

    app_dir = click.get_app_dir(APP_NAME)
    logger.debug("App dir: %s", app_dir)
    ctx.obj = {}
    ctx.obj["app_dir"] = app_dir
    ctx.obj["config_file"] = os.path.join(ctx.obj["app_dir"], "config.yml")
    ctx.obj["config"] = {}
    ctx.obj["verbose"] = verbose
    if os.path.exists(ctx.obj["config_file"]):
        logger.debug("Loading config from %s", ctx.obj["config_file"])
        with open(ctx.obj["config_file"]) as f:
            ctx.obj["config"] = yaml.load(f, Loader=yaml.FullLoader)

    if ctx.invoked_subcommand is None:
        ctx.invoke(setup)


@main.command()
@click.pass_obj
def setup(obj):
    if not os.path.exists(obj["app_dir"]):
        os.makedirs(obj["app_dir"])

    if not os.path.exists(obj["config_file"]):
        with open(obj["config_file"], "w") as f:
            f.write("# empty")


@main.command()
@click.option("--dry-run", "-s", is_flag=True)
@click.option("--create/--no-create", default=True)
@click.option("--prune/--no-prune", default=True)
@click.option("--info/--no-info", default=True)
@click.option("--progress", is_flag=True)
@click.pass_obj
def backup(obj, dry_run, create, prune, info, progress):
    tasks = obj["config"]["tasks"]
    logger.info("Handling %d tasks", len(tasks))
    for task in tasks:
        with tempfile.NamedTemporaryFile("w+") as ex_f:
            source = os.path.expandvars(os.path.expanduser(task["source"]))
            logger.info("Source: %s", source)
            logger.debug(
                "Writing %d exclude patterns to %s",
                len(task["exclude_patterns"]),
                ex_f.name,
            )
            ex_f.write(
                "\n".join(
                    [
                        os.path.expanduser(os.path.expandvars(e))
                        for e in task["exclude_patterns"]
                    ]
                )
            )
            ex_f.flush()

            for repo in task["repositories"]:
                url = repo["url"]
                exe = repo.get("executable", "borg")
                args = repo.get("extra_args", {})
                archive_name = f"{url}::{task['archive_name']}"
                logger.info("Destination: %s", archive_name)

                progress = progress and sys.stdout.isatty()
                fg = progress

                if create:
                    logger.info("Creating archive...")
                    spinner = None
                    if not progress:
                        spinner = Halo(text="Creating archive")
                        spinner.start()
                    try:
                        borg.create(
                            archive_name,
                            source,
                            progress=progress,
                            exclude_from=ex_f.name,
                            remote_path=exe,
                            dry_run=dry_run,
                            _fg=fg,
                            **args,
                        )
                        pass
                    finally:
                        if spinner is not None:
                            spinner.stop()

                    logger.info("Archive created")

                if prune:
                    retention = task["retention"]
                    logger.info(
                        "Pruning: %s",
                        ", ".join(f"{k}: {v}" for k, v in retention.items()),
                    )

                    extra = {}
                    if obj["verbose"] >= 1:
                        extra["stats"] = True
                        extra["list"] = True

                    with Halo(text=f"Pruning archives at {url}"):
                        prune = borg.prune(
                            url,
                            H=retention["hourly"],
                            d=retention["daily"],
                            w=retention["weekly"],
                            m=retention["monthly"],
                            y=retention["yearly"],
                            remote_path=exe,
                            dry_run=dry_run,
                            _err_to_out=True,
                            **extra,
                        )
                    print(prune)

                if info:
                    with Halo(text=f"Getting info on {url}"):
                        info = borg.info(url, remote_path=exe)
                    print(info)
