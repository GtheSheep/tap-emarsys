# tap-emarsys

`tap-emarsys` is a Singer tap for Emarsys.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

```bash
pipx install git+https://github.com/gthesheep/tap-emarsys
```

## Configuration

### Accepted Config Options

`username` - Username for your Emarsys API user
`secret` - Secret for your Emarsys API user
`language_id` - Identifier of the language for your account, i.e. `en` (default)

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-emarsys --about
```

### Source Authentication and Authorization

See [the Emarsys docs](https://dev.emarsys.com/docs/emarsys-api/ZG9jOjI0ODk5NzAx-authentication) for details on
how to obtain authentication credentials

## Usage

You can easily run `tap-emarsys` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-emarsys --version
tap-emarsys --help
tap-emarsys --config CONFIG --discover > ./catalog.json
```

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_emarsys/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-emarsys` CLI interface directly using `poetry run`:

```bash
poetry run tap-emarsys --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-emarsys
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-emarsys --version
# OR run a test `elt` pipeline:
meltano elt tap-emarsys target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
