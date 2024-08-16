# NPD_Scripts
Scripts to help parse data from the National Public Data breach.
All were generated with ChatGPT - Not written by me.

# Contents
- Extract_SingleState.py - Extracts a single state, change the script to your target state.
- Extract_AllStates.py - Extracts every state into their own .csv
- AllState_Extract_FAST.py - Extracts all states at once, processing 1m lines/chunk.
- WebUI - A Flask server and WebUI (Generated by ChatGPT, not me) to search a SQLite database.

# WebUI Note:
The WebUI was written to target trimmed down data, you will have to either trim down your extracted data or modify the WebUI index and Webserver.py to target full dataset columns)
