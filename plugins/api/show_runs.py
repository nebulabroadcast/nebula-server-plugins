from typing import Any

from pydantic import Field

import nebula
from server.dependencies import CurrentUser
from server.models import RequestModel, ResponseModel
from server.request import APIRequest


# ContextPluginResponseModel and Context will become a
# part of server.models in Nebula 6.0.2 final
# for now, we use the local copy of the model


class ContextPluginResponseModel(ResponseModel):
    type: str = "table"
    header: str | None = None
    footer: str | None = None
    dialog_style: dict[str, Any] = {}
    payload: dict[str, Any] = {}


class ContextPluginRequestModel(RequestModel):
    id_asset: int = Field(..., description="Asset ID")


# Actual plugin code

class ShowRunsPlugin(APIRequest):
    """Show past and scheduled runs for an asset

    This plugin is scoped to the asset page, so once activated,
    user can execute it by selecting 'Show runs' from the action (cog) menu.
    in the asset page.
    """

    name: str = "show_runs"
    title: str = "Show runs"
    scopes = ["asset"]
    response_model = ContextPluginResponseModel

    async def handle(
        self,
        request: ContextPluginRequestModel,
        user: CurrentUser,
    ) -> ContextPluginResponseModel:

        asset = await nebula.Asset.load(request.id_asset)

        if not user.can("asset_view", asset["id_folder"]):
            raise nebula.ForbiddenException()

        query = """
            SELECT
                i.id as id,
                e.meta as emeta,
                r.start as rstart
            FROM
                items AS i
            INNER JOIN
                events AS e
                ON e.id_magic = i.id_bin
            LEFT JOIN
                asrun AS r
                ON r.id_item = i.id
            WHERE
                i.id_asset = $1
        ORDER BY e.start ASC
        """

        data = []

        async for row in nebula.db.iterate(query, asset.id):
            event = nebula.Event.from_meta(row["emeta"])
            data.append(
                {
                    "id": row["id"],
                    "event_title": event["title"],
                    "event_time": event["start"],
                    "run_time": row["rstart"],
                }
            )

        columns = [
            {
                "name": "event_title",
                "title": "Event title",
            },
            {
                "name": "event_time",
                "title": "Event time",
                "type": "datetime",
                "width": 150,
            },
            {
                "name": "run_time",
                "title": "Broadcast time",
                "type": "datetime",
                "width": 150,
            },
        ]

        return ContextPluginResponseModel(
            type="table",
            header=asset.title,
            dialog_style={"width": 700, "height": "80%"},
            payload={"columns": columns, "data": data},
        )
