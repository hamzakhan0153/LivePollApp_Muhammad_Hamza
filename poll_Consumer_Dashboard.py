import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
from collections import Counter


def main():
    st.title("Live Polling Real-time Dashboard")

    # Kafka Consumer
    consumer = KafkaConsumer(
        'LivePoll',
        bootstrap_servers="localhost:9092",
        session_timeout_ms=45000,
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    records = []
    q1_counter = Counter()
    q2_counter = Counter()
    q3_counter = Counter()

    #placeholder for chart
    metric_placeholder = st.empty()
    q1_chart_placeholder = st.empty()
    q2_chart_placeholder = st.empty()
    q3_chart_placeholder = st.empty()
    table_placeholder = st.empty()

    if 'main_df' not in st.session_state:
        st.session_state.main_df = pd.DataFrame(columns=['Q1', 'Q2', 'Q3'])

    q1_order = ['1', '2', '3', '4', '5']
    q2_order = ['V. Bad', 'Bad', 'Moderate', 'Good', 'V. Good']
    q3_order = ['Yes', 'No']

    st.toast("Listening to Kafka stream...")

    #loop for realtime display of output
    for msg in consumer:

        if msg is None:
            continue

        data = msg.value
        records.append(data)

        # Extract answers for q1,q2,q3
        answers = data.get("answer_array", [])
        
        q1 = list(answers[0].values())[0]
        q1_counter[q1] += 1

        q2 = list(answers[1].values())[0]
        q2_counter[q2] += 1

        q3 = list(answers[2].values())[0]
        q3_counter[q3] += 1
        

        total_responses = len(records)

        # total responses count
        with metric_placeholder.container():
            st.metric("Total Responses", total_responses)


        # Q1 Chart
        q1_df = pd.DataFrame({
            "Rating": q1_order,
            "Count": [q1_counter.get(x, 0) for x in q1_order]
        })

        with q1_chart_placeholder.container():
            st.subheader("Q1: Conference Overall Rating")
            st.bar_chart(q1_df.set_index("Rating"))

        # Q2 Chart
        q2_df = pd.DataFrame({
            "Rating": q2_order,
            "Count": [q2_counter.get(x, 0) for x in q2_order]
        })

        with q2_chart_placeholder.container():
            st.subheader("Q2: keynote speaker Rating")
            st.bar_chart(q2_df.set_index("Rating"))

        # Q3 Chart 
        q3_df = pd.DataFrame({
            "Rating": q3_order,
            "Count": [q3_counter.get(x, 0) for x in q3_order]
        })

        with q3_chart_placeholder.container():
            st.subheader("Q3: breakout sessions")
            st.bar_chart(q3_df.set_index("Rating"))

        # data frame
        #print(answers)
        answer_id = data.get("answer_id")
        #print(answer_id)
        df = pd.DataFrame({
            'Q1' : [list(answers[0].values())[0]],
            'Q2' : [list(answers[1].values())[0]],
            'Q3' : [list(answers[2].values())[0]]
        },index=[answer_id])

        df.index.name = 'Answer_id'
        st.session_state.main_df = pd.concat([st.session_state.main_df, df])

        with table_placeholder.container():
            st.subheader("DataFrame")
            st.dataframe(st.session_state.main_df)

#footer 
def add_footer(): 
    footer = """ 
    <style> 
    .footer 
    { 
    position: fixed; 
    left: 0; bottom: 0; 
    width: 100%; 
    background-color: white; 
    color: black; 
    text-align: center; 
    padding: 10px; 
    font-size: 25px; 
    z-index: 9999; 
    } 
    </style> 
    <div class="footer"> 
    <p>'App created by Muhammad Hamza'</p> 
    </div> """ 
    st.markdown(footer, unsafe_allow_html=True)

if __name__ == "__main__":
    add_footer()
    main()