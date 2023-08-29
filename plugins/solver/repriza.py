import nebula


class Plugin(nebula.plugins.SolverPlugin):
    name = "repriza"

    async def solve(self):
        # Get previous folder of same name

        query = """
            SELECT e.meta as meta
            FROM events e
            WHERE e.id_channel = $1
            AND e.start < $2
            AND e.meta->>'title' = $3
            ORDER BY e.start DESC
            LIMIT 1
        """

        nebula.log.trace(f"Solving reprise for {self.event['title']}")

        res = await nebula.db.fetch(
            query,
            self.event["id_channel"],
            self.event["start"],
            self.event["title"],
        )

        if not res:
            return

        prev_event = nebula.Event.from_meta(res[0]["meta"])
        nebula.log.trace(f"Found previous {prev_event}")

        query = """
            SELECT
                i.meta as imeta,
                a.meta as ameta
            FROM items i
            LEFT JOIN assets a ON i.id_asset = a.id
            WHERE i.id_bin =  $1
            ORDER BY i.position ASC
        """

        async for row in nebula.db.iterate(query, prev_event["id_magic"]):
            item = nebula.Item.from_meta(row["imeta"])
            item["id"] = None
            if ameta := row["ameta"]:
                asset = nebula.Asset.from_meta(ameta)

                # skip commercials
                if asset["id_folder"] == 9:
                    continue

                item.asset = asset

            nebula.log.trace(f"Cloning {item}")
            yield item
