package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MyFakebookOracle extends FakebookOracle {

    static String prefix = "tajik.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding tables in your database
    String cityTableName = null;
    String userTableName = null;
    String friendsTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;
    String programTableName = null;
    String educationTableName = null;
    String eventTableName = null;
    String participantTableName = null;
    String albumTableName = null;
    String photoTableName = null;
    String coverPhotoTableName = null;
    String tagTableName = null;


    // DO NOT modify this constructor
    public MyFakebookOracle(String dataType, Connection c) {
        super();
        oracleConnection = c;
        // You will use the following tables in your Java code
        cityTableName = prefix + dataType + "_CITIES";
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITY";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITY";
        programTableName = prefix + dataType + "_PROGRAMS";
        educationTableName = prefix + dataType + "_EDUCATION";
        eventTableName = prefix + dataType + "_USER_EVENTS";
        albumTableName = prefix + dataType + "_ALBUMS";
        photoTableName = prefix + dataType + "_PHOTOS";
        tagTableName = prefix + dataType + "_TAGS";
    }


    @Override
    // ***** Query 0 *****
    // This query is given to your for free;
    // You can use it as an example to help you write your own code
    //
    public void findMonthOfBirthInfo() {

        // Scrollable result set allows us to read forward (using next())
        // and also backward.
        // This is needed here to support the user of isFirst() and isLast() methods,
        // but in many cases you will not need it.
        // To create a "normal" (unscrollable) statement, you would simply call
        // Statement stmt = oracleConnection.createStatement();
        //
        try (Statement stmt =
                     oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)) {

            // For each month, find the number of users born that month
            // Sort them in descending order of count
            ResultSet rst = stmt.executeQuery("select count(*), month_of_birth from " +
                    userTableName +
                    " where month_of_birth is not null group by month_of_birth order by 1 desc");

            this.monthOfMostUsers = 0;
            this.monthOfLeastUsers = 0;
            this.totalUsersWithMonthOfBirth = 0;

            // Get the month with most users, and the month with least users.
            // (Notice that this only considers months for which the number of users is > 0)
            // Also, count how many total users have listed month of birth (i.e., month_of_birth not null)
            //
            while (rst.next()) {
                int count = rst.getInt(1);
                int month = rst.getInt(2);
                if (rst.isFirst())
                    this.monthOfMostUsers = month;
                if (rst.isLast())
                       this.monthOfLeastUsers = month;
                this.totalUsersWithMonthOfBirth += count;
            }

            // Get the names of users born in the "most" month
            rst = stmt.executeQuery("select user_id, first_name, last_name from " +
                    userTableName + " where month_of_birth=" + this.monthOfMostUsers);
            while (rst.next()) {
                Long uid = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                this.usersInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
            }

            // Get the names of users born in the "least" month
            rst = stmt.executeQuery("select first_name, last_name, user_id from " +
                    userTableName + " where month_of_birth=" + this.monthOfLeastUsers);
            while (rst.next()) {
                String firstName = rst.getString(1);
                String lastName = rst.getString(2);
                Long uid = rst.getLong(3);
                this.usersInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
            }

            // Close statement and result set
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 1 *****
    // Find information about users' names:
    // (1) The longest first name (if there is a tie, include all in result)
    // (2) The shortest first name (if there is a tie, include all in result)
    // (3) The most common first name, and the number of times it appears (if there
    //      is a tie, include all in result)
    //
    public void findNameInfo() { // Query1
        // Find the following information from your database and store the information as shown
        /*
        this.longestFirstNames.add("JohnJacobJingleheimerSchmidt");
        this.shortestFirstNames.add("Al");
        this.shortestFirstNames.add("Jo");
        this.shortestFirstNames.add("Bo");
        this.mostCommonFirstNames.add("John");
        this.mostCommonFirstNames.add("Jane");
        this.mostCommonFirstNamesCount = 10;
        */

        // Scrollable result set allows us to read forward (using next())
        // and also backward.
        // This is needed here to support the user of isFirst() and isLast() methods,
        // but in many cases you will not need it.
        // To create a "normal" (unscrollable) statement, you would simply call
        // Statement stmt = oracleConnection.createStatement();
        //
        try (Statement stmt =
                     oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)) {

            // For each user, count up the instances of each first name
            // and group count by first name in descending order
            ResultSet rst = stmt.executeQuery("SELECT COUNT(first_name) as num_first, first_name from " + userTableName +
                    " WHERE first_name IS NOT NULL GROUP BY first_name ORDER BY num_first DESC");

            int max = 0;
            while(rst.next()) {
                String first_name = rst.getNString(2);
                if(rst.isFirst()) {
                    max = rst.getInt(1);
                    this.mostCommonFirstNames.add(first_name);
                    this.mostCommonFirstNamesCount = max;
                }
                // In case of ties
                // If max is equal to the current row's count and not the first row, add it
                else if(rst.getInt(1) == max) this.mostCommonFirstNames.add(first_name);

                // If max is not equal to the current row's count, skip to the end
                else rst.last();
            }
            rst.close();

            // For each user, get the length of the first name and the first name
            // group by first name and order in desc order by length of first name
            rst = stmt.executeQuery("SELECT length(first_name) as namelength, first_name FROM " + userTableName + 
                    " WHERE first_name IS NOT NULL GROUP BY first_name ORDER BY namelength DESC");
            //int num_rows_in_rst = 0;
            int len = 0;
             while(rst.next()) {
                String first_name = rst.getNString(2);
                // Since first row is going to be first name that is longest, add to container
                if(rst.isFirst()) {
                    this.longestFirstNames.add(first_name);
                    len = rst.getInt(1);
                }
                // In case of ties
                // If there are rows after the first that have the same length
                else if(rst.getInt(1) == len) this.longestFirstNames.add(first_name);

                else rst.last();
            }

            rst.afterLast(); // Known redundancy

            while(rst.previous()) {
                String first_name = rst.getString(2);

                if (rst.isLast()) {
                    this.shortestFirstNames.add(first_name);
                    len = rst.getInt(1);
                }
                else if (rst.getInt(1) == len) this.shortestFirstNames.add(first_name);
                else rst.first();
            }

            // Close statement and result set
            rst.close();
            stmt.close();
        
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 2 *****
    // Find the user(s) who have no friends in the network
    //
    // Be careful on this query!
    // Remember that if two users are friends, the friends table
    // only contains the pair of user ids once, subject to
    // the constraint that user1_id < user2_id
    //
    public void lonelyUsers() {
        // Find the following information from your database and store the information as shown
        //this.lonelyUsers.add(new UserInfo(10L, "Billy", "SmellsFunny"));
        //this.lonelyUsers.add(new UserInfo(11L, "Jenny", "BadBreath"));
    
        try (Statement stmt =
                     oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)) {

            ResultSet rst = stmt.executeQuery("SELECT user_id, first_name, last_name FROM " + userTableName +
                " WHERE NOT EXISTS (SELECT * FROM " + friendsTableName + " WHERE user_id = user1_id OR user_id = user2_id)");

            while(rst.next()) {
                long user_id = rst.getLong(1);
                String first_name = rst.getNString(2);
                String last_name = rst.getNString(3);
                this.lonelyUsers.add(new UserInfo(user_id, first_name, last_name));
            }

            rst.close();
            stmt.close();

        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 3 *****
    // Find the users who do not live in their hometowns
    // (I.e., current_city != hometown_city)
    //
    public void liveAwayFromHome() throws SQLException {
        //this.liveAwayFromHome.add(new UserInfo(11L, "Heather", "Movalot"));

        try (Statement stmt =
                 oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                         ResultSet.CONCUR_READ_ONLY)) {

            ResultSet rst = stmt.executeQuery("SELECT U.user_id, U.first_name, U.last_name FROM " + userTableName +
                " U, " + currentCityTableName + " C, " + hometownCityTableName + " H " + 
                "WHERE U.user_id = C.user_id AND U.user_id = H.user_id AND C.current_city_id != H.hometown_city_id " +
                "ORDER BY u.user_id ASC");

                while(rst.next()) {
                    long user_id = rst.getLong(1);
                    String first_name = rst.getNString(2);
                    String last_name = rst.getNString(3);
                    this.liveAwayFromHome.add(new UserInfo(user_id, first_name, last_name));
                }

            rst.close();
            stmt.close();

        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // **** Query 4 ****
    // Find the top-n photos based on the number of tagged users
    // If there are ties, choose the photo with the smaller numeric PhotoID first
    //
    public void findPhotosWithMostTags(int n) {

        try (Statement stmt =
                 oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                         ResultSet.CONCUR_READ_ONLY)) {

                stmt.executeUpdate("CREATE VIEW photo_num_subjects (num_subjects, photo_id) AS SELECT COUNT(*) as numsubjects, tag_photo_id FROM " + tagTableName + 
                        " WHERE tag_photo_id IS NOT NULL GROUP BY tag_photo_id ORDER BY numsubjects DESC, tag_photo_id ASC");

                stmt.executeUpdate("CREATE VIEW photos_with_most_subjects (photo_id) AS SELECT photo_id FROM photo_num_subjects WHERE ROWNUM <= " + n);

                ResultSet rst = stmt.executeQuery("SELECT ms.photo_id, p.album_id, a.album_name, p.photo_caption, p.photo_link FROM photos_with_most_subjects ms, " + 
                    photoTableName + 
                    " p, " +
                    albumTableName +
                    " a WHERE ms.photo_id = p.photo_id AND p.album_id = a.album_id ORDER BY ms.photo_id asc");

                while(rst.next()) {
                    String photo_id = rst.getNString(1);
                    String album_id = rst.getNString(2);
                    String album_name = rst.getNString(3);
                    String photo_caption = rst.getNString(4);
                    String photo_link = rst.getNString(5);

                    PhotoInfo p = new PhotoInfo(photo_id, album_id, album_name, photo_caption, photo_link);
                    TaggedPhotoInfo tp = new TaggedPhotoInfo(p);

                    try (Statement stmt_two = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                         ResultSet.CONCUR_READ_ONLY)) {

                        ResultSet rst_two = stmt_two.executeQuery("SELECT u.user_id, u.first_name, u.last_name FROM photos_with_most_subjects ms, " +
                            userTableName + " u, " + tagTableName + " t WHERE ms.photo_id = " + photo_id + 
                            " AND ms.photo_id = t.tag_photo_id AND t.tag_subject_id = u.user_id ORDER BY u.user_id asc");

                        while(rst_two.next()) {
                            long user_id = rst_two.getLong(1);
                            String first_name = rst_two.getNString(2);
                            String last_name = rst_two.getNString(3);
                            tp.addTaggedUser(new UserInfo(user_id, first_name, last_name));
                        }

                        rst_two.close();
                        stmt_two.close();
                    } catch (SQLException err) {
                        System.err.println(err.getMessage());
                    }
                    
                    this.photosWithMostTags.add(tp);
                }

                stmt.executeUpdate("DROP VIEW photo_num_subjects");
                stmt.executeUpdate("DROP VIEW photos_with_most_subjects");

                rst.close();
                stmt.close();

        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // **** Query 5 ****
    // Find suggested "match pairs" of users, using the following criteria:
    // (1) One of the users is female, and the other is male
    // (2) Their age difference is within "yearDiff"
    // (3) They are not friends with one another
    // (4) They should be tagged together in at least one photo
    //
    // You should return up to n "match pairs"
    // If there are more than n match pairs, you should break ties as follows:
    // (i) First choose the pairs with the largest number of shared photos
    // (ii) If there are still ties, choose the pair with the smaller user_id for the female
    // (iii) If there are still ties, choose the pair with the smaller user_id for the male
    //
    public void matchMaker(int n, int yearDiff) {

        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_READ_ONLY)) {

            stmt.executeUpdate("CREATE VIEW pairs (u1_id, u1_g, u1_f, u1_l, u1_y, u2_id, u2_g, u2_f, u2_l, u2_y, p_id, p_aid, p_c, a_n, p_l) AS SELECT U1.user_id, U1.gender, U1.first_name, U1.last_name, U1.year_of_birth, U2.user_id, U2.gender, U2.first_name, U2.last_name, U2.year_of_birth, P.photo_id, P.album_id, P.photo_caption, A.album_name, P.photo_link FROM " + 
                userTableName + " U1, " + userTableName + " U2, " + photoTableName + " P, " + 
                tagTableName + " T1, " + tagTableName + " T2, " + albumTableName + " A " + 
                "WHERE U1.user_id != U2.user_id AND U1.gender != U2.gender AND U1.gender = 'female' AND ABS(U1.year_of_birth - U2.year_of_birth) <= " + yearDiff +
                " AND U1.user_id = T1.tag_subject_id AND T1.tag_photo_id = T2.tag_photo_id AND " +
                " T2.tag_subject_id = U2.user_id AND U2.user_id NOT IN (SELECT F.user2_id FROM " +
                friendsTableName + " F WHERE U1.user_id = F.user1_id) AND U1.user_id NOT IN (SELECT F.user2_id FROM " + 
                friendsTableName + " F WHERE U2.user_id = F.user1_id) AND P.photo_id = T1.tag_photo_id AND P.album_id = A.album_id");

            stmt.executeUpdate("CREATE VIEW numphotos (u1, u2, sum) AS SELECT d1.u1_id, d1.u2_id, COUNT(d1.u1_id) AS banana " + 
                "FROM pairs d1, pairs d2 WHERE (d1.u1_id = d2.u1_id AND d1.u2_id = d2.u2_id) " + 
                "GROUP BY d1.u1_id, d1.u2_id ORDER BY banana desc, d1.u1_id asc, d1.u2_id asc");

            ResultSet rst = stmt.executeQuery("SELECT D.u1_id, D.u1_g, D.u1_f, D.u1_l, D.u1_y, D.u2_id, D.u2_g, D.u2_f, D.u2_l, D.u2_y, " + 
            "D.p_id, D.p_aid, D.p_c, D.a_n, D.p_l, L.U1, L.U2, L.SUM " + 
            "FROM pairs D, numphotos L WHERE D.u1_id = L.U1 AND D.u2_id = L.U2 " + 
            "GROUP BY D.u1_id, D.u1_g, D.u1_f, D.u1_l, D.u1_y, D.u2_id, D.u2_g, D.u2_f, D.u2_l, D.u2_y, D.p_id, D.p_aid, D.p_c, D.a_n, D.p_l, L.U1, L.U2, L.SUM " + 
            "ORDER BY L.SUM DESC, L.U1 ASC, L.U2 ASC");

            Long previousU1 = 0L;
            Long previousU2 = 0L;
            int row = 0;

            MatchPair mp = null;
            int count = 0;

            while(rst.next() && count < n) {
                Long girlUserId = rst.getLong(1);
                String u1_gender = rst.getNString(2);
                String girlFirstName = rst.getNString(3);
                String girlLastName = rst.getNString(4);
                int girlYear = rst.getInt(5);
                Long boyUserId = rst.getLong(6);
                String u2_gender = rst.getNString(7);
                String boyFirstName = rst.getNString(8);
                String boyLastName = rst.getNString(9);
                int boyYear = rst.getInt(10);

                String sharedPhotoId = rst.getNString(11);
                String sharedPhotoAlbumId = rst.getNString(12);
                String sharedPhotoAlbumName = rst.getNString(14);
                String sharedPhotoCaption = rst.getNString(13);
                String sharedPhotoLink = rst.getNString(15);

                System.out.print("R\n" + " " + girlUserId + " " + u1_gender + " " + girlFirstName + " " + girlLastName + " " + girlYear + " " + boyUserId + " " + u2_gender + " " + boyFirstName + " " + boyLastName + " " + boyYear);

                if(girlUserId == previousU1 && boyUserId == previousU2) {
                    mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId,
                            sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
                    this.bestMatches.add(mp);
                }
                else if ((girlUserId != previousU1 && boyUserId != previousU2) || row == 1) {
                    mp = new MatchPair(girlUserId, girlFirstName, girlLastName,
                        girlYear, boyUserId, boyFirstName, boyLastName, boyYear);
                    mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId,
                            sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
                    this.bestMatches.add(mp);
                    ++count;
                }

                row++;
                previousU1 = girlUserId;
                previousU2 = boyUserId;
            }

            stmt.executeUpdate("drop view numphotos");
            stmt.executeUpdate("drop view pairs");

            rst.close();
            stmt.close();

        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    // **** Query 6 ****
    // Suggest users based on mutual friends
    //
    // Find the top n pairs of users in the database who have the most
    // common friends, but are not friends themselves.
    //
    // Your output will consist of a set of pairs (user1_id, user2_id)
    // No pair should appear in the result twice; you should always order the pairs so that
    // user1_id < user2_id
    //
    // If there are ties, you should give priority to the pair with the smaller user1_id.
    // If there are still ties, give priority to the pair with the smaller user2_id.
    //
    @Override
    public void suggestFriendsByMutualFriends(int n) {

        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_READ_ONLY)) {

            // Collect all non-friends and all of their mutual friends
            stmt.executeUpdate("CREATE VIEW Apple (user1_id, user1_first, user1_last, user2_id, user2_first, user2_last, mutual_id, mutual_first, mutual_last) AS " +
                "SELECT U1.user_id, U1.first_name, U1.last_name, U2.user_id, U2.first_name, U2.last_name, U.user_id,  U.first_name, U.last_name " +
                "FROM " + userTableName + " U1, " + userTableName + " U2, " + userTableName + " U " +
                "WHERE U.user_id != U1.user_id AND U.user_id != U2.user_id AND U1.user_id < U2.user_id " +
                "AND NOT EXISTS (SELECT * FROM " + friendsTableName + " F WHERE F.user1_id = U1.user_id AND F.user2_id = U2.user_id) " +
                "AND (U.user_id IN (SELECT F.user1_id FROM " + friendsTableName +  " F WHERE F.user2_id = U1.user_id) " +
                    "OR U.user_id IN (SELECT F.user2_id FROM " + friendsTableName + " F WHERE F.user1_id = U1.user_id)) " +
                "AND (U.user_id IN (SELECT F.user1_id FROM " + friendsTableName + " F WHERE F.user2_id = U2.user_id)" +
                    "OR U.user_id IN (SELECT F.user2_id FROM " + friendsTableName + " F WHERE F.user1_id = U2.user_id))");

            // Collect all non-friends with mutual friends, count their number of mutual friends and sort by that count and above constraints
            stmt.executeUpdate("CREATE VIEW Banana (user1_id, user2_id, sum) AS " +
                "SELECT M.user1_id, M.user2_id, count(M.mutual_id) AS sum " +
                "FROM Apple M " +
                "GROUP BY M.user1_id, M.user2_id ORDER BY sum DESC, M.user1_id ASC, M.user2_id ASC");

            // Reduce size of Banana to run faster (difference between running n*(total number of mutual friends for all pairs) and
            // (number of non-friends with mutual friends)*(total number of mutual friends for all pairs). Big difference with small n
            stmt.executeUpdate("CREATE VIEW ORANGE (user1_id, user2_id, sum) AS " +
                "SELECT B.user1_id, B.user2_id, B.sum " +
                " FROM Banana B WHERE ROWNUM <= " + (n + 1) +
                " ORDER BY B.sum DESC, B.user1_id ASC, B.user2_id ASC");
            
            ResultSet rst = stmt.executeQuery("SELECT M.user1_id, M.user1_first, M.user1_last, M.user2_id, M.user2_first, M.user2_last, M.mutual_id, M.mutual_first, M.mutual_last " +
                "FROM Apple M, Orange S " +
                "WHERE M.user1_id = S.user1_id AND M.user2_id = S.user2_id " +
				"ORDER BY S.sum DESC, M.user1_id ASC, M.user2_id ASC, M.mutual_id ASC");
            
            Boolean next = rst.next(); // To have more control over when it is incremented (specifically to not accidentally skip a row)
            // Track when the next pair has been reached
            Long prev_u1_id = 0L;
            Long prev_u2_id = 0L;
            
			int count = 0;

            while(next && count < n) {
				Long u1_id = rst.getLong(1);
            	String u1_first = rst.getNString(2);
            	String u1_last = rst.getNString(3);
            	Long u2_id = rst.getLong(4);
            	String u2_first = rst.getNString(5);
            	String u2_last = rst.getNString(6);
				prev_u1_id = u1_id;
                prev_u2_id = u2_id;

                UsersPair p = new UsersPair(u1_id, u1_first, u1_last, u2_id, u2_first, u2_last);
				while (prev_u1_id == rst.getLong(1) && prev_u2_id == rst.getLong(4) && next) {
					Long m_id = rst.getLong(7);
                	String m_first = rst.getNString(8);
                	String m_last = rst.getNString(9);
					p.addSharedFriend(m_id, m_first, m_last);
                    next = rst.next();
				}
                
                this.suggestedUsersPairs.add(p);
				++count;
            }

            stmt.executeUpdate("drop view Apple");
            stmt.executeUpdate("drop view Banana");
            stmt.executeUpdate("drop view Orange");

            rst.close();
            stmt.close();

        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 7 *****
    //
    // Find the name of the state with the most events, as well as the number of
    // events in that state.  If there is a tie, return the names of all of the (tied) states.
    //
    public void findEventStates() {

        // "select c.state_name, count(*) as num_events from tajik.public_user_events, tajik.public_cities c where event_city_id = c.city_id group by num_events order by num_events desc;"

        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_READ_ONLY)) {

            stmt.executeUpdate("CREATE VIEW States (state, count) AS SELECT STATE_NAME, Count(State_name) AS Num_Events FROM " +
            eventTableName + ", " + cityTableName + " WHERE CITY_ID = EVENT_CITY_ID GROUP BY STATE_NAME ORDER BY Num_Events DESC");

            ResultSet rst = stmt.executeQuery("SELECT state, count FROM States WHERE count = (SELECT MAX(count) FROM States)");

            while(rst.next()) {
                int event_count = rst.getInt(2);
                String state_name = rst.getNString(1);

                this.eventCount = event_count;
                this.popularStateNames.add(state_name);
            }
            stmt.executeUpdate("DROP VIEW States");

            rst.close();
            stmt.close();

        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    //@Override
    // ***** Query 8 *****
    // Given the ID of a user, find information about that
    // user's oldest friend and youngest friend
    //
    // If two users have exactly the same age, meaning that they were born
    // on the same day, then assume that the one with the larger user_id is older
    //
    public void findAgeInfo(Long user_id) {

        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_READ_ONLY)) {


            stmt.executeUpdate("CREATE VIEW AgeF (id, first_name, last_name, year, month, day) AS " +
                "SELECT U2.user_id, U2.first_name, U2.last_name, U2.year_of_birth, U2.month_of_birth, U2.day_of_birth " +
                "FROM " + friendsTableName + ", " + userTableName + " U, " + userTableName + " U2 " +
                "WHERE U.user_id = " + user_id + " AND ((user1_id = U.user_id AND user2_id = U2.user_id) OR " +
                    "(user2_id = U.user_id AND user1_id = U2.user_id)) AND U.user_id != U2.user_id " +
                "GROUP BY U2.user_id, U2.first_name, U2.last_name, U2.year_of_birth, U2.month_of_birth, U2.day_of_birth " +
                "ORDER BY U2.year_of_birth DESC, U2.month_of_birth DESC, U2.day_of_birth DESC, U2.user_id DESC");

            // Youngest Friend
            ResultSet rst = stmt.executeQuery("SELECT A.id, A.first_name, A.last_name FROM AgeF A, AgeF Ad, AgeF Am, AgeF Ay " +
                "WHERE A.id = Ad.id AND A.id = Am.id AND a.id = Ay.id AND Ay.year = (SELECT MAX(year) FROM AgeF) AND " +
                "Am.month = (SELECT MAX(month) FROM AgeF WHERE year = Ay.year) AND " +
                "Ad.day = (SELECT MAX(day) FROM AgeF WHERE year = Ay.year AND Am.month = month)");

            while(rst.next()) {
                Long young_user_id = rst.getLong(1);
                String young_first_name = rst.getNString(2);
                String young_last_name = rst.getNString(3);

                this.youngestFriend = new UserInfo(young_user_id, young_first_name, young_last_name);
            }
            rst.close();

            // Oldest Friend
            rst = stmt.executeQuery("SELECT A.id, A.first_name, A.last_name FROM AgeF A, AgeF Ad, AgeF Am, AgeF Ay " +
                "WHERE A.id = Ad.id AND A.id = Am.id AND a.id = Ay.id AND Ay.year = (SELECT MIN(year) FROM AgeF) AND " +
                "Am.month = (SELECT MIN(month) FROM AgeF WHERE year = Ay.year) AND " +
                "Ad.day = (SELECT MIN(day) FROM AgeF WHERE year = Ay.year AND Am.month = month)");

            while(rst.next()) {
                Long old_user_id = rst.getLong(1);
                String old_first_name = rst.getNString(2);
                String old_last_name = rst.getNString(3);

                this.oldestFriend = new UserInfo(old_user_id, old_first_name, old_last_name);
            }

            stmt.executeUpdate("drop view AgeF");
            
            rst.close();
            stmt.close();

        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    //	 ***** Query 9 *****
    //
    // Find pairs of potential siblings.
    //
    // A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
    // if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
    // on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
    //
    //
    public void findPotentialSiblings() {
        /*
        Long user1_id = 123L;
        String user1FirstName = "User1FirstName";
        String user1LastName = "User1LastName";
        Long user2_id = 456L;
        String user2FirstName = "User2FirstName";
        String user2LastName = "User2LastName";
        SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
        this.siblings.add(s);
        */

        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_READ_ONLY)) {

            ResultSet rst = stmt.executeQuery("SELECT user1_id, user2_id, u1.first_name, u1.last_name, u2.first_name, u2.last_name " +
                "FROM " + friendsTableName + " , " + userTableName + " u1, " + userTableName + " u2, " +
                hometownCityTableName + " h1, " + hometownCityTableName + " h2 " +
                "WHERE user1_id = u1.user_id AND user2_id = u2.user_id AND u1.last_name = u2.last_name AND " +
                "(ABS(u1.year_of_birth - u2.year_of_birth) < 10) AND u1.user_id != u2.user_id AND u1.user_id = h1.user_id AND " +
                "u2.user_id = h2.user_id AND h1.hometown_city_id = h2.hometown_city_id " +
                "GROUP BY user1_id, user2_id, u1.first_name, u1.last_name, u2.first_name, u2.last_name " +
                "ORDER BY user1_id ASC, user2_id ASC");

            while(rst.next()) {
                Long user1_id = rst.getLong(1);
                Long user2_id = rst.getLong(2);
                String user1_first_name = rst.getNString(3);
                String user1_last_name = rst.getNString(4);
                String user2_first_name = rst.getNString(5);
                String user2_last_name = rst.getNString(6);
                SiblingInfo s = new SiblingInfo(user1_id, user1_first_name, user1_last_name, user2_id, user2_first_name, user2_last_name);
                this.siblings.add(s);
            }

            rst.close();
            stmt.close();

        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

}