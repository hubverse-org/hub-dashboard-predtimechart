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
1. **Visualization data files**: This project will configure predtimechart to load its data from `.json` files that will be generated from hub forecast files, an approach similar to how [viz.covid19forecasthub.org](https://viz.covid19forecasthub.org) works ([GitHub repo](https://github.com/reichlab/Covid-19-Hub-Vizualization)). This requires us to write a program (we will use Python) to generate those `.json` files, like the R files [here](https://github.com/reichlab/Covid-19-Hub-Vizualization/tree/master/preprocess_data). The `.json` files will be stored in the [AWS S3](https://aws.amazon.com/s3/) bucket for each hub, akin to how [hubverse-transform](https://github.com/hubverse-org/hubverse-transform) saves its generated `.parquet` files to S3. Our initial constraints:
    - `output_type`: To start we will only support hubs that contain `quantile` forecasts (please see [Output types](https://hubverse.io/en/latest/user-guide/tasks.html#output-types) in the docs).
    - `intervals`: @todo
1. **Predtimechart configuration**: Predtimechart is configured via a JavaScript [options object](https://github.com/reichlab/predtimechart?tab=readme-ov-file#options-object) that specifies settings like `available_as_ofs`, `task_ids`, `models`, etc. Our current thinking is that this object will be generated from [hub configuration files](https://hubverse.io/en/latest/user-guide/hub-config.html).
    - generation details (reference_date -> as_of/selected date, horizon, target_date: x axis, task id vars -> dropdowns, ...): @todo
1. **Server/Dashboard**: We will write a simple dashboard/landing page providing a link to the forecast visualization (predtimechart) page. Our initial thought is to implement this via a straighforward [S3 static website](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WebsiteHosting.html) (i.e., a self-contained `index.html` file, perhaps with some JavaScript to access basic [hubverse admin](https://hubverse.io/en/latest/quickstart-hub-admin/intro.html) information to orient the viewer such as hub name, tasks summary, etc.)

# Testing

We plan to use the https://github.com/hubverse-org/example-complex-forecast-hub for development unit tests.

# Questions/issues

- How/when will file generation be triggered? This applies to both `.json` visualization files and the predtimechart configuration object. For example, and admin UI, GitHub Action on schedule, round close, etc.
- Is this a good time to remove predtimechart's user ensemble, if desired?
- Is this an opportunity to set up some kind of general purpose notification service for interested parties (e.g., hub admins) that informs them when, say, the viz is configured or updated, viz data files are updated, etc.?
