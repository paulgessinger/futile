import click
import os
import yaml
import logging
import sys
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor

import coloredlogs

from .borg import borg
from .util import render_info, Spinner, handle_backup_task

APP_NAME = "futile"

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
    elif verbose == 2:
        level = logging.DEBUG
    else:
        level = logging.DEBUG
        global_level = logging.DEBUG

    coloredlogs.install(
        fmt="%(asctime)s %(levelname)s %(name)s %(filename)s:%(funcName)s %(message)s",
        level=level,
    )

    logging.getLogger("sh.command").setLevel(logging.WARNING)

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
    verbose = obj["verbose"]
    logger.info("Handling %d tasks", len(tasks))
    for task in tasks:
        for repo in task["repositories"]:
            handle_backup_task(
                task, repo, logger, verbose, dry_run, create, prune, info, progress
            )


@main.command()
@click.pass_obj
def info(obj):
    tasks = obj["config"]["tasks"]
    logger.info("Handling %d tasks", len(tasks))
    for task in tasks:
        for repo in task["repositories"]:
            render_info(repo)


@main.command()
@click.option("--dry-run", "-s", is_flag=True)
@click.option("--create/--no-create", default=True)
@click.option("--prune/--no-prune", default=True)
@click.option("--info/--no-info", default=True)
@click.option("--progress", is_flag=True)
@click.pass_obj
def schedule(obj, dry_run, create, prune, info, progress):
    for name in ["apscheduler.scheduler", "apscheduler.executors.default"]:
        l = logging.getLogger(name)
        l.setLevel(logger.getEffectiveLevel())

    executors = {"default": ThreadPoolExecutor(1)}
    job_defaults = {"coalesce": True, "max_instances": 1}
    scheduler = BlockingScheduler(executors=executors, job_defaults=job_defaults)

    def job(task, repo, logger, verbose, dry_run, create, prune, info, progress):
        logger.info("Running scheduled job")
        handle_backup_task(
            task, repo, logger, verbose, dry_run, create, prune, info, progress
        )
        time.sleep(65)

    tasks = obj["config"]["tasks"]
    verbose = obj["verbose"]
    logger.info("Preparing scheduling of %d tasks", len(tasks))

    for task in tasks:
        repos = task["repositories"]
        logger.info("Task has %d target repositories", len(repos))
        for repo in repos:
            crontab = repo["schedule"]
            logger.debug("Scheduling backup task with: %s", crontab)
            args = (task, repo, logger, verbose, dry_run, create, prune, info, progress)
            scheduler.add_job(job, args=args, trigger=CronTrigger.from_crontab(crontab))

    try:
        logger.info("Starting scheduler")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
