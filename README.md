An interview home assignment.

# Sessionization

the main file - [lib/sessionization/sessionizator.ex](https://github.com/ChadoNihi/sessionizator/blob/master/lib/sessionization/sessionizator.ex)

## Requirements

- Erlang 20 (the previous versions have not been tested)
- [Elixir >= 1.5.x if you want to rebuild the script or run the tests]

## Example Usage

First, `cd sessionizator`. Then

```
# read events from the file
./sessionize -f "/home/electrofish/Projects/sessionizator/sample_data/dataset_tiny.json"
```

...or

```
# read events line by line from stdin
./sessionize
```

Too slow? Try `./sessionize-fast` instead.

### Build

Set `MIX_ENV` to `prod` and then

```
cd sessionizator
mix deps.get
mix escript.build
```

### Test (TODO)

```
cd sessionizator
mix test
```
