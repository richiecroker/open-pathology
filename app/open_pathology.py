import measures
import streamlit
import pandas as pd

streamlit.set_page_config(layout="wide")

cb_safe = streamlit.toggle("Color-blind friendly colors", value=False)

streamlit.markdown(
    """
    <style>
        .block-container {
            max-width: 80%;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

@streamlit.cache_resource
def get_repository():
    return measures.OSJobsRepository()


def main():
    repository = get_repository()

    streamlit.title("OpenPathology")

    selected_measure_name = streamlit.selectbox("Select a measure:", repository.list())

    measure = repository.get(selected_measure_name)

    streamlit.header(measure.name)

    streamlit.markdown(
        "The codes used for this measure "
        f"are available in [this codelist]({measure.codelist_url})."
    )

    with streamlit.expander("What is it and why does it matter?"):
        streamlit.markdown(measure.explanation)

    with streamlit.expander("Design"):
        streamlit.markdown(measure.design)

    with streamlit.expander("Caveats"):
        streamlit.markdown(measure.caveats)

    chart = measure.deciles_chart(cb_safe=cb_safe)
    streamlit.altair_chart(chart, use_container_width=True)

    streamlit.subheader("Demographic breakdowns")

    if measure.measures_tables:
        selected_demographic = streamlit.selectbox(
            "Select a demographic breakdown:", sorted(measure.measures_tables.keys())
        )

        streamlit.altair_chart(measure.measure_chart(selected_demographic))

    else:
        streamlit.markdown("No demographic breakdowns are available.")

    streamlit.subheader(f"Most common codes ([codelist]({measure.codelist_url}))")

    streamlit.dataframe(measure.top_5_codes_table)

    streamlit.markdown(f"Total events: {measure.total_events:,} events")



if __name__ == "__main__":
    main()
