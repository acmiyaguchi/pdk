# pdk
Pilosa Dev Kit - implementation tooling and use case examples are here!

Documentation is here: https://www.pilosa.com/docs/pdk/

First, get Pilosa running: https://www.pilosa.com/docs/getting-started/

Clone, install with `make install`, then run `pdk` and follow usage instructions.

## Taxi usecase

To get started immediately, run this:

`pdk taxi`

This will create and fill an index called `taxi`, using the short url list in usecase/taxi/urls-short.txt.

If you want to try out the full data set, run this:

`pdk taxi -d taxi-big -f usecase/taxi/urls.txt`

Note that this url file represents 1+ billion rows of data.

After importing, you can try a few example queries at https://github.com/alanbernstein/pilosa-notebooks/blob/master/taxi-use-case.ipynb .

## Net usecase

To get started immediately, run this:

`pdk net -i en0`

which will capture traffic on the interface `en0` (see available interfaces with `ifconfig`).
