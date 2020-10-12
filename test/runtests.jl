using Test
using MicropredictionHistory

@test MicropredictionHistory.loadStream("/Users/rusty/Development/pluto/data",  "emojitracker-twitter-grinning_face_with_smiling_eyes.json") !== missing