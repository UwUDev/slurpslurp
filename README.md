# SlurpSlurp

SlurpSlurp is a discord data harvester. 
It uses discord tokens to suck up all data passing through discord accounts (messages, users, files, images, embeds, guilds, channels, roles...).

This project lets you “back-up” your account from the moment you start, or collect massive amounts of data. You can then make extremely fast and accurate local searches everywhere at once.

Other tools are available in the [tools](./tools) folder, such as a live image viewer, invites extractor and a data preparer for fine-tuning LLMs.

- [SlurpSlurp](#slurpslurp)
- [Running](#running)
    * [Configuration](#configuration)
    * [Compiling](#compiling)
- [Tools](#tools)
    * [Image Viewer](#image-viewer)
    * [Dataset generator](#dataset-generator)
        + [Args](#args)
- [Database](#database)
    * [Current Schema](#current-schema)
        + [DB Optimizations](#db-optimizations)

# Running

To run SlurpSlurp, you need to have an updated version of rust with cargo installed.

You can then run the following command to build and run the project:

```bash
cargo run --release
```

## Configuration

You can configure SlurpSlurp by creating a `config.toml` file in the root directory of the project based on [config_example.toml](./config_example.toml).

You'll also need to set up a [PostgreSQL database](#database) to store the collected data. 

To configure the accounts you want to use, you can create a `tokens.txt` file in the root directory of the project. Each line in this file should contain a Discord token that you want to use for data collection.

## Compiling
To compile SlurpSlurp, you can use the following command:

```bash
cargo build --release
```

You'll then find the binary in the `target/release` directory.

# Tools

You can find various tools in the [tools](./tools) directory. These tools are designed to help you with different tasks related to SlurpSlurp, such as viewing images, preparing data for fine-tuning LLMs, and more.

Make sure you have installed the required dependencies in the [requirements.txt](./tools/requirements.txt) file before running the tools.

## Image Viewer

The image viewer tool allows you to view live images from the data collected by SlurpSlurp. You can run it using the following command:

```bash
cd tools
python image_viewer.py
```

The web server will start, and you can access the image viewer in your web browser at `http://localhost:8882`.

![Image Viewer](./assets/img_viewer.png)

## Dataset generator

The dataset generator tool allows you to prepare data for fine-tuning LLMs. You can run it using the following command:

```bash
cd tools
python prepare_dataset.py "postgresql://postgres:postgres@localhost:5432/slurpslurp" train_data.jsonl validation_data.jsonl --split-ratio 0.1
```

### Args

- `postgresql://postgres:postgres@localhost:5432/slurpslurp`: The connection string to your PostgreSQL database.
- `train_data.jsonl`: The output file for the training data.
- `validation_data.jsonl`: The output file for the validation data.
- `--split-ratio 0.1`: The ratio of the dataset to be used for validation (default is 0.1, meaning 10% of the data will be used for validation).

## Invites extractor

The invites extractor tool allows you to extract invites from the collected data. You can run it using the following command:

```bash
cd tools
python invites_extractor.py "postgresql://postgres:postgres@localhost:5432/slurpslurp"
```

This will output a txt file containing all the unique invites codes found in the collected data.

# Database

SlurpSlurp uses PostgreSQL as its database to store the collected data. You can set up your PostgreSQL database [here](https://www.postgresql.org/docs/current/tutorial-install.html).

## Current Schema

See [setup.sql](./sql_scripts/setup.sql).

### DB Optimizations

To optimize the database for SlurpSlurp, you can add [TimeScaleDB](https://docs.timescale.com/latest/getting-started/installation) to your PostgreSQL instance. This will allow you to handle faster parallel writes and queries.
