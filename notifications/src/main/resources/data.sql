INSERT INTO SELECT_REQUESTS(QUERY,RECEIVER,QUERY_DESCRIPTION,MESSAGE)
VALUES ('SELECT * FROM leaderboard WHERE score>=500', 'Participant', 'Notify when any person gets at least 500 points.','You went up at least 500 points! CONGRATS!');
INSERT INTO SELECT_REQUESTS(QUERY,RECEIVER,QUERY_DESCRIPTION,MESSAGE)
VALUES ('SELECT * FROM leaderboard WHERE place=1', 'Participant', 'Notify when any person climbs at least 1 position.','You went up at least 1 position! CONGRATS!');
INSERT INTO SELECT_REQUESTS(QUERY,RECEIVER,QUERY_DESCRIPTION,MESSAGE)
VALUES ('SELECT * FROM leaderboard WHERE place=-1', 'Participant', 'Notify when any person goes down at least 1 position.','You went down at least 1 position! Get back in there!');
