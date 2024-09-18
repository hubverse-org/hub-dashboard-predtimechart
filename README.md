# hub-dashboard-predtimechart

# Goal

Create an initial dashboard that provides a [predtimechart](https://github.com/reichlab/predtimechart)-based forecast visualization component for the hub. The thinking is that this will allow us to get something practical into the hands of hubverse users relatively quickly.

Considerations:

- Client side only, i.e., nothing server-side. This will greatly simplify new hub onboarding.
- Others: @todo

# Implementation summary

The major parts of this project are:

1. **Forecast visualization component**: To visualize forecast data, we will generalize the [predtimechart](https://github.com/reichlab/predtimechart) JavaScript component to work with [Hubverse](https://github.com/hubverse-org) hubs. Details:
    - specific component changes: @todo
2. **Visualization data files**: This project will configure predtimechart to load its data from `.json` files that will be generated from hub forecast files, an approach similar to how [viz.covid19forecasthub.org](https://viz.covid19forecasthub.org) works ([GitHub repo](https://github.com/reichlab/Covid-19-Hub-Vizualization)). This requires us to write a program (we will use Python) to generate those `.json` files, like the R files [here](https://github.com/reichlab/Covid-19-Hub-Vizualization/tree/master/preprocess_data). The `.json` files will be stored in the [AWS S3](https://aws.amazon.com/s3/) bucket for each hub, akin to how [hubverse-transform](https://github.com/hubverse-org/hubverse-transform) saves its generated `.parquet` files to S3. Our initial constraints:
    - `output_type`: To start we will only support hubs that contain `quantile` forecasts (please see [Output types](https://hubverse.io/en/latest/user-guide/tasks.html#output-types) in the docs).
    - `intervals`: @todo
3. **Predtimechart configuration**: Predtimechart is configured via a JavaScript [options object](https://github.com/reichlab/predtimechart?tab=readme-ov-file#options-object) that specifies settings like `available_as_ofs`, `task_ids`, `models`, etc. Our current thinking is that this object will be generated from [hub configuration files](https://hubverse.io/en/latest/user-guide/hub-config.html).
    - generation details (reference_date -> as_of/selected date, horizon, target_date: x axis, task id vars -> dropdowns, ...): @todo
4. **Server/Dashboard**: We will write a simple dashboard page providing a link to the forecast visualization (predtimechart) page. Our initial thought is to implement this via a straighforward [S3 static website](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WebsiteHosting.html) (i.e., a self-contained `index.html` file, perhaps with some JavaScript to access basic [hubverse admin](https://hubverse.io/en/latest/quickstart-hub-admin/intro.html) information to orient the viewer such as hub name, tasks summary, etc.) Two comparable sites are https://respicast.ecdc.europa.eu/ (especially) and https://covid19forecasthub.org/ . See [Dashboard architecture] below for details.

# Assumptions/limitations

Initially the visualization will have these limitations:

- Only one round block in `tasks.json > rounds` can be plotted.
- Only one `model_tasks` group within that round block can be plotted, and only `model_tasks` groups with `quantile` [output_types](https://hubverse.io/en/latest/user-guide/model-output.html#formats-of-model-output) will be considered.
- The hub has `reference_date`|`origin_date` and `target_date`|`target_end_date` task IDs in `tasks.json > rounds > model_tasks > task_ids`.
- Only forecast data will be plotted, not target data.
- Model metadata must contain a boolean `designated_model` field.
- Only one `target_keys` key in `tasks.json > /rounds/_/model_tasks/_/target_metadata/_/target_keys/` is supported, and only one entry within it.
- We assume all hub files have been validated.
- For the `task_ids` entry in predtimechart config option generation, we use `value` for both `value` and `text`, rather than asking the user to provide a mapping from `value` to `text`. A solution is to require that mapping in `predtimechart-config.yml`.
- The `initial_as_of` and `current_date` config fields are the last of `hub_config.fetch_reference_dates`.
- The `initial_task_ids` config field is the first `task_ids` `value`.
- The following quantile levels (`output_type_id`s) are present in the data: 0.025, 0.25, 0.5, 0.75, 0.975
- others: @todo

# Required hub configuration

Some visualization-related information must be configured for each hub, including:

- which interval levels to show. initially: None, 50%, 95%
- which round block in `tasks.json` to use
- reference_date column name
- target_date column name
- name of boolean field for model inclusion. initially we will assume it is `designated_model`
- names of hub models - to be listed first
- `initial_checked_models` (a predtimechart option)
- others: @todo

# Dashboard architecture

Our initial thinking is an approach where we provide a fixed layout (e.g., a menubar at top and a content area in the middle, such as found at https://respicast.ecdc.europa.eu/ ) that allows limited customization [specified by convention](https://en.wikipedia.org/wiki/Convention_over_configuration) via markdown files (some with specific names) placed in directories with specific names. Details:

- Configurable content is specified via markdown files located in a directory named `hub-website` (say) in the root hub directory.
- The site layout is a single column (100% width) with two rows: A menubar/header at the top, and a content area taking up the rest of the vertical space.
- The menubar contains these items (from left to right): Home (brand image/text), "Forecasts", "Evaluations", "Background", "Community", "Get in touch".
- The content area depends on the selected menu item:
    - Home: Content is loaded from `hub-website/home.md`.
    - "Forecasts": Content is the predtimechart visualization.
    - "Evaluations": @todo
    - "Background", "Community", "Get in touch": @todo loaded from specific files under `hub-website` such as `background.md`, etc.

# Testing

We plan to primarily use https://github.com/hubverse-org/example-complex-forecast-hub for development unit tests.

# Questions/issues

- How/when will file generation be triggered? This applies to both `.json` visualization files and the predtimechart configuration object. For example, and admin UI, GitHub Action on schedule, round close, etc.
- Is this a good time to remove predtimechart's user ensemble, if desired?
- Is this an opportunity to set up some kind of general purpose notification service for interested parties (e.g., hub admins) that informs them when, say, the viz is configured or updated, viz data files are updated, etc.?
- Dashboard: Do we want to allow users to add menu items that link to pages with content loaded from .md files? For example, should we support a `hub-website/menus` where users can put files that become menu items with the file name (capitalized, say) and content generated from the file.
- Generation/scheduling: We will need a flag to indicate whether we want to regenerate forecast json files for all past weeks, or only for the present week.
- Where is the source data coming from - GitHub vs. S3?
- Which model output formats will we support? The hub docs mention CSV and parquet, but that others (e.g., zipped files) might be supported.
- Regarding naming the .json files, should we be influenced by Arrow's partitioning scheme where it names intermediate directories according to filtering.
- We might need separate apps to update config options vs. visualization data (json files) for the case where the user has changed predtimechart-config.yml independent of a round closing. 
- Should we filter out `hub_config.horizon_col_name == 0` ?
- Should `forecast_data_for_model_df()`'s `quantile_levels` be stored in a config file somewhere?

# Python local dev setup

Use the following to create a local dev setup using pyenv and pipenv, which we assume are already installed.

## Set up the virtual environment

```bash
$ cd <this repo>
$ pyenv versions  # you should see this repo's .python-version set
$ pipenv --python $(pyenv which python)
```

## Generate requirements-dev.txt (for pipenv, etc.)

```bash
$ pipenv install pip-tools  # for `pip-compile`
$ pipenv run pip-compile --extra=dev --output-file=requirements/requirements.txt pyproject.toml
```

## Install required packages

```bash
$ cd <this repo>
$ pipenv install -r requirements/requirements.txt -e .
```

## Run the tests and application

```bash
$ cd <this repo>
$ pipenv run python -m pytest
$ pipenv run python src/hub_predtimechart/app.py
```
