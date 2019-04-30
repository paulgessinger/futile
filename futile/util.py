import click
import dateutil.parser
import json
from halo import Halo
import contextlib
import sys
import tempfile
import os
import sh
from datetime import datetime
import subprocess
from urllib.parse import urlparse

from .borg import borg


def ping(host):
    try:
        subprocess.check_call(
            ["ping", "-c", "1", host],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except:
        return False


@contextlib.contextmanager
def Spinner(text, persist=True, *args, **kwargs):
    stream = kwargs.get("stream", sys.stdout)
    if stream.isatty() and Halo is not None:
        spinner = Halo(text, *args, **kwargs)
        spinner.start()
        try:
            yield
            if persist:
                spinner.succeed()
        except:
            if persist:
                spinner.fail()
            raise
        finally:
            if not persist:
                spinner.stop()
    else:
        sys.stdout.write(text + "\n")
        yield


def render_info(repo):
    url = repo["url"]
    exe = repo.get("executable", "borg")
    with Spinner(text=f"Getting info on {url}"):
        archives = json.loads(str(borg.list(url, remote_path=exe, json=True)))
        info = json.loads(str(borg.info(url, remote_path=exe, json=True)))

    width, _ = click.get_terminal_size()

    archive_rows = []

    for archive in archives["archives"]:
        name = archive["name"]
        start = dateutil.parser.parse(archive["start"])
        archive_rows.append((name, start))

    name_width = max([len(n) for n, _ in archive_rows])
    click.echo(width * "-")
    click.secho(f"{url}", bold=True)

    click.echo("-" * name_width + " " + (width - name_width - 1) * "-")

    for n, d in archive_rows:
        click.echo(
            n.ljust(name_width)
            + " "
            + d.strftime("%a, %Y-%m-%d %H:%M:%s").rjust(width - name_width - 1)
        )

    click.echo("-" * name_width + " " + (width - name_width - 1) * "-")

    total_size = info["cache"]["stats"]["total_size"]
    total_csize = info["cache"]["stats"]["total_csize"]
    unique_csize = info["cache"]["stats"]["unique_csize"]

    # to GB
    GB = 1e9
    total_size /= GB
    total_csize /= GB
    unique_csize /= GB

    # print(total_size, total_csize, unique_csize)

    orig_label = "Original size"
    comp_label = "Compressed size"
    uniq_label = "Deduplicated size"
    gap = " " * 4
    click.echo(orig_label + gap + comp_label + gap + uniq_label)
    click.secho(
        f"{total_size:.2f} GB".rjust(len(orig_label)), fg="bright_red", nl=False
    )
    click.echo(gap, nl=False)
    click.secho(
        f"{total_csize:.2f} GB".rjust(len(comp_label)), fg="bright_yellow", nl=False
    )
    click.echo(gap, nl=False)
    click.secho(
        f"{unique_csize:.2f} GB".rjust(len(uniq_label)), fg="green", bold=True, nl=False
    )

    click.echo("\n")


def handle_backup_task(
    task, repo, logger, verbose, dry_run, create, prune, info, progress
):
    start = datetime.now()

    logger.debug("Checking for pre exec script")
    pre_exec = repo.get("pre_exec", None)
    logger.debug("Pre exec is: %s", pre_exec)
    if pre_exec is not None:
        if os.path.exists(pre_exec):
            logger.debug("Pre exec is script file (probably). Not supported yet.")
        elif pre_exec == "ping":
            # host, _ = repo["url"].split(":", 1)
            o = urlparse(repo["url"])
            host = o.hostname
            logger.debug("Pinging %s before kicking off backup", host)
            if not ping(host):
                logger.info("Pinging %s unsuccessful, skipping execution", host)
                return
            logger.info("Ping ok")
        else:
            logger.debug("Pre exec is string, evaling")
            loc = {}
            exec(pre_exec, globals(), loc)
            ret = loc["func"](task, repo)
            if ret == False:
                logger.info("Pre exec returned 'False', skipping execution")
                return

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

        url = repo["url"]
        exe = repo.get("executable", "borg")
        args = repo.get("extra_args", {})
        archive_name = f"{url}::{task['archive_name']}"
        logger.info("Destination: %s", archive_name)

        progress = progress and sys.stdout.isatty()
        fg = progress

        try:
            if create:
                logger.info("Creating archive...")
                with Spinner(text="Creating archive", enabled=not progress):
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

                logger.info("Archive created")

            if prune:
                retention = task["retention"]
                logger.info(
                    "Pruning: %s", ", ".join(f"{k}: {v}" for k, v in retention.items())
                )

                extra = {}
                if verbose >= 2:
                    extra["stats"] = True
                    extra["list"] = True

                with Spinner(text=f"Pruning archives at {url}"):
                    prune = borg.prune(
                        url,
                        H=retention["hourly"],
                        d=retention["daily"],
                        w=retention["weekly"],
                        m=retention["monthly"],
                        y=retention["yearly"],
                        remote_path=exe,
                        dry_run=dry_run,
                        # _err_to_out=True,
                        **extra,
                    )
                logger.info(prune)

            if info:
                if verbose >= 1:
                    render_info(repo)

            td = datetime.now() - start
            logger.info("Completed backup in %ds", td.seconds)
        except sh.ErrorReturnCode_1 as e:
            logger.warn("Backup creation raised warning:")
            logger.debug(e.full_cmd)
            logger.debug("Exit code: %d", e.exit_code)
            logger.debug("STDERR: %s", e.stderr.decode("utf-8"))
            if len(e.stdout) > 0:
                logger.debug("STDOUT: %s", e.stdout.decode("utf-8"))
            # logger.debug(str(e))
        except sh.ErrorReturnCode_2 as e:
            logger.error("Backup creation failed, skipping")
            logger.debug(e.full_cmd)
            logger.debug("Exit code: %d", e.exit_code)
            logger.debug("STDERR: %s", e.stderr.decode("utf-8"))
            if len(e.stdout) > 0:
                logger.debug("STDOUT: %s", e.stdout.decode("utf-8"))
            # logger.debug(str(e))
