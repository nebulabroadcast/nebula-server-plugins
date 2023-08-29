import os
import nebula
import time

from nxtools import format_time, format_filesize


async def get_scheduled_assets(id_channel: int) -> list[int]:
    """Return a list of assets scheduled for playback"""

    query = """
        SELECT
            DISTINCT(i.id_asset) as id_asset
        FROM
            items AS i,
            events AS e
        WHERE
            e.id_channel = $1
        AND e.start > $2
        AND e.start < $3
        AND e.id_magic = i.id_bin
    """

    FROM = time.time() - (5 * 24 * 3600)
    TO = time.time() + (14 * 24 * 3600)

    result = []
    async for row in nebula.db.iterate(query, id_channel, FROM, TO):
        result.append(row["id_asset"])
    return result


async def get_last_run(id_asset: int, id_channel: int) -> float:
    query = """
        SELECT r.start as last_run
        FROM asrun AS r, items AS i
        WHERE r.id_channel = $1
        AND r.id_item = i.id
        AND i.id_asset = $2
        ORDER BY start DESC LIMIT 1
    """
    res = await nebula.db.fetch(query, id_channel, id_asset)
    if not res:
        return 0
    return res[0]["last_run"]


async def clear():
    for channel in nebula.settings.playout_channels:
        nebula.log.info(f"Cleaning {channel.name} storage")
        scheduled_assets = await get_scheduled_assets(channel.id)

        playout_dir = os.path.join(
            nebula.storages[channel.playout_storage].local_path,
            channel.playout_dir,
        )

        query = f"""
            SELECT meta FROM assets
            WHERE meta->>'playout_status/{channel.id}' IS NOT NULL
        """

        i = 0
        s = 0
        async for row in nebula.db.iterate(query):
            asset = nebula.Asset.from_row(row)
            last_run = await get_last_run(asset.id, channel.id)

            if asset.id in scheduled_assets:
                continue

            if last_run > time.time() - (3600 * 24 * 14):
                continue
            i += 1

            s += asset[f"playout_status/{channel.id}"]["size"]

            playout_path = os.path.join(
                playout_dir, f"{nebula.config.site_name}-{asset.id}.mxf"
            )

            nebula.log.debug(f"Removing {asset}. Last aired {format_time(last_run)}")

            os.remove(playout_path)
            del asset.meta[f"playout_status/{channel.id}"]
            await asset.save()

        nebula.log.info(
            f"Removed {i} {channel.name} playout media files. "
            f"{format_filesize(s) or '0 bytes'} freed."
        )


class PlayoutCleaner(nebula.plugins.CLIPlugin):
    """Clean playout storage"""

    name = "playout_cleaner"

    async def main(self, id_channel: int = 1):
        await clear()
