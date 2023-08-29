import nebula


class AiredMarker(nebula.plugins.CLIPlugin):
    name = "aired"

    async def main(self):
        """Mark broadcasted items as aired.

        This plugin is meant to be run by cron. It will mark all items
        that have been broadcasted as aired (meta.aired = true),
        so they can be easily filtered.
        
        Usage:

        docker compose exec backend python -m cli aired
        """

        query = """
        UPDATE assets SET
        meta = jsonb_set(
            jsonb_set(meta, '{aired}', 'true', true),
            '{mtime}',
            to_jsonb(extract(epoch from now())),
            true
        ),
        mtime=extract(epoch from now())
        WHERE id IN (
          SELECT i.id_asset
          FROM items i
          INNER JOIN asrun r ON i.id = r.id_item
        )
        AND (meta->>'aired' IS NULL OR meta->>'aired' = 'false');
        """

        await nebula.db.execute(query)
