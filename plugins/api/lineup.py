import time
import datetime

from fastapi import Query, Response
from pydantic import Field

import nebula

from nebula.settings.models import PlayoutChannelSettings

from server.request import APIRequest
from server.models import ResponseModel


def get_from_time(channel_config: PlayoutChannelSettings) -> int:
    now = datetime.datetime.now()
    hh, mm = channel_config.day_start

    desired_time = datetime.time(hh, mm, 0)
    desired_datetime = datetime.datetime.combine(now.date(), desired_time)

    unix_timestamp = int(time.mktime(desired_datetime.timetuple()))

    if now < desired_datetime:  # early morning
        unix_timestamp -= 3600 * 24

    return unix_timestamp


class ScheduleEvent(ResponseModel):
    start: int = Field(
        ...,
        title="Start time",
        description="Unix timestamp of event start",
        example=f"{int(time.time())}",
    )
    title: str | None = Field(
        None,
        title="Title",
        description="Event title",
        example="Star Trek IV",
    )
    subtitle: str | None = Field(
        None,
        title="Subtitle",
        description="Subtitle. If present, append to title. Display whenever possible.",
        example="The voyage home",
    )
    summary: str | None = Field(
        None,
        title="Summary",
        description="Short description of the block",
        example="Epic sci-fi saga",
    )
    description: str | None = Field(
        None,
        title="Description",
        example="Captain Kirk and his crew have to travel "
        "back in time to reverse the disastrous effects "
        "caused by an unknown space probe on planet earth.",
    )
    idec: str | None = Field(
        None,
        title="IDEC",
        example="A4206988",
    )
    promoted: bool | None = Field(
        False,
        title="Promoted",
        description="Highlight this item if possible",
    )


class ScheduleResponse(ResponseModel):
    channel_id: int = Field(...)
    channel_name: str = Field(..., example="A11")
    events: list[ScheduleEvent] = Field(default_factory=list)


class Lineup(APIRequest):
    """Get a schedule of a channel"""

    name: str = "lineup"
    title: str = "Lineup"
    response_model = ScheduleResponse
    methods = ["GET"]

    async def handle(
        self,
        response: Response,
        id_channel: int = Query(1),
        # user: nebula.User = Depends(current_user),
    ):
        """Get a list of assignments for the current user"""

        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "*"

        playout_config = nebula.settings.get_playout_channel(id_channel)
        assert playout_config is not None, f"No such channel ID {id_channel}"

        events: list[ScheduleEvent] = []

        start_ts = get_from_time(playout_config)
        end_ts = start_ts + (3600 * 24 * 7)

        query = """
            SELECT
                start,
                meta->>'title' AS title,
                meta->>'subtitle' AS subtitle,
                meta->>'summary' AS summary,
                meta->>'description' AS description,
                meta->>'promoted' AS promoted,
                meta->>'id/main' AS idec
            FROM events
            WHERE id_channel = $1
            AND start >= $2
            AND start < $3
            ORDER BY start ASC
        """

        async for row in nebula.db.iterate(query, id_channel, start_ts, end_ts):
            events.append(ScheduleEvent(**row))

        return ScheduleResponse(
            channel_name=playout_config.name,
            channel_id=id_channel,
            events=events,
        )
