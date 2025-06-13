import os


INSTRUMENTS = {
    "LATISS": "AuxTel",
    "LSSTCam" : "Simonyi",
}


def filter_tickets_by_instrument(tickets, instrument):

    def matches_and_add_url(ticket):
        # Get the list of systems from the object
        obj_system_list = ticket['system']
        search_terms = (instrument, INSTRUMENTS[instrument])
        # Check if any search term appears in any system name
        matched = any(term in system for term in search_terms for system in obj_system_list)
        if matched:
            ticket['url'] = f"https://{os.environ.get('JIRA_API_HOSTNAME')}/browse/{ticket.get('key')}"
            return True
        return False

    return [ticket for ticket in tickets if matches_and_add_url(ticket)]


