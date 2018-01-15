import mincemeat

client = mincemeat.Client()
client.password	= 'dontforgetme'
client.conn('localhost', mincemeat.DEFAULT_PORT)
