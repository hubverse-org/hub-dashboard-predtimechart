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


# Dashboard architecture

Our initial thinking is an approach where we provide a fixed layout (e.g., a menubar at top and a content area in the middle, such as found at https://respicast.ecdc.europa.eu/ ) that allows limited customization [specified by convention](https://en.wikipedia.org/wiki/Convention_over_configuration) via markdown files (some with specific names) placed in directories with specific name. Details:
- Configurable content is specified via markdown files located in a directory named `hub-website` (say) in the root hub directory.
- The site layout is a single column (100% width) with two rows: A menubar/header at the top, and a content area taking the rest of the vertical space.
- The menubar contains these items (from left to right): Home (brand image/text), "Forecasts", "Evaluations", "Background", "Community", "Get in touch".
- The content area depends on the selected menu item:
  - Home: Content is loaded from `hub-website/home.md`.
  - "Forecasts": Content is the predtimechart visualization.
  - "Evaluations": @todo
  - "Background", "Community", "Get in touch": @todo loaded from specific files under `hub-website` such as `background.md`, etc.


# Testing

We plan to use the https://github.com/hubverse-org/example-complex-forecast-hub for development unit tests.

# Questions/issues

- How/when will file generation be triggered? This applies to both `.json` visualization files and the predtimechart configuration object. For example, and admin UI, GitHub Action on schedule, round close, etc.
- Is this a good time to remove predtimechart's user ensemble, if desired?
- Is this an opportunity to set up some kind of general purpose notification service for interested parties (e.g., hub admins) that informs them when, say, the viz is configured or updated, viz data files are updated, etc.?
- Dashboard: Do we want to allow users to add menu items that link to pages with content loaded from .md files? For example, should we support a `hub-website/menus` where users can put files that become menu items with the file name (capitalized, say) and content generated from the file.
