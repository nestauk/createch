# Various utilities that we use across the pipeline


def has_createch_sector(comm_number, vector, comm_container, thres=0.04):
    """Checks if a project has topics from a createch sector"""

    has_comm_number = any(vector[top] > thres for top in comm_container[comm_number])
    if has_comm_number is True:
        return True
    else:
        return False
