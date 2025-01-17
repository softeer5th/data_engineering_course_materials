import datetime as dt

def write_log(full_name:str, state:str, is_start: bool=True, is_error: bool=False, msg: Exception=None):
    with open(full_name, 'a') as log:
        time = dt.datetime.now().strftime('%Y-%b-%d-%H-%M-%S')
        if is_error:
            log.write(f'{time}, [{state}] Failed\n\t{msg}\n')
        elif is_start:
            log.write(f'{time}, [{state}] Started\n')
        else:
            log.write(f'{time}, [{state}] Ended\n')