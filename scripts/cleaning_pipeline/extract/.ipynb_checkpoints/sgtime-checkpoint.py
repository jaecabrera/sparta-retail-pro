from datetime import datetime

import pytz


def now() -> str:
    """
    :description:
            Get current timezone for SG (Singapore) and return it as
        date formatted string of 'month + day + year + '-' +
        hour + minutes + seconds'
    :return: Date formatted string.
    """
    tz_sg = pytz.timezone('Singapore')
    datetime_sg = datetime.now(tz_sg)

    return datetime_sg.strftime('%m%d%Y-%H%M')
