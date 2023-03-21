# CSCE 608 Project 1
## By Chris Baldwin

### Requirements
- MySQL
- Python 3.11
- MySQL Python Connector
- Pipenv

### Installing
To start, we source the pipenv shell and install the dependencies.
```bash
$ pipenv shell
$ pipenv install
```
Next we need to add the login info for the MySQL credentials
```bash 
$ python install.py \
    --host <host> \
    --user <user> \
    --password <password> \
    --database <database>
```
Now we can run the download and ingest scripts
```bash
$ python install.py \ 
    --download \
    --ingest \
    [--num_lines <num_lines>] # can be used to limit the number of lines to ingest
    
```
Other options are available for the download and ingest scripts. Run `python install.py --help` for more information.

Finally, we can run the frontend
```bash
$ python main.py [--debug] [--port <port>]
```

You can view the frontend at `http://localhost:5001/` by default or `http://localhost:<port>/` if specified.
