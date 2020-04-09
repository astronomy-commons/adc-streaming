from urllib.parse import urlparse


def parse_kafka_url(val):
    """Extracts the group ID, broker addresses, and topic names from a Kafka URL.

    The URL should be in this form:
        ``kafka://[groupid@]broker[,broker2[,...]]/topic[,topic2[,...]]``

    The returned group ID and topic may be None if they aren't in the URL.

    """
    parsed = urlparse(val)
    if parsed.scheme != "kafka":
        raise ValueError("invalid kafka URL: must start with 'kafka://'")

    split_netloc = parsed.netloc.split("@", maxsplit=1)
    if len(split_netloc) == 2:
        group_id = split_netloc[0]
        broker_addresses = split_netloc[1].split(",")
    else:
        group_id = None
        broker_addresses = split_netloc[0].split(",")

    topics = parsed.path.lstrip("/")
    if len(topics) == 0:
        split_topics = None
    else:
        split_topics = topics.split(",")
    return group_id, broker_addresses, split_topics
