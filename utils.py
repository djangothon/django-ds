# Utils

def retry(func, retries, *args, **kwargs):
    tries = 0
    ret_value = None
    while tries < retries:
        try:
            ret_val = func(*args, **kwargs)
            break
        except:
            tries += 1

    return ret_val

