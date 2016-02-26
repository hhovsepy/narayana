/*
 * JBoss, Home of Professional Open Source
 * Copyright 2006, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 *
 * (C) 2005-2006,
 * @author JBoss Inc.
 */
/*
 * Copyright (C) 2000, 2001,
 *
 * Arjuna Solutions Limited,
 * Newcastle upon Tyne,
 * Tyne and Wear,
 * UK.
 *
 * $Id: JDBCImple.java 2342 2006-03-30 13:06:17Z  $
 */

package com.arjuna.ats.internal.arjuna.objectstore.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import javax.naming.NamingException;

import com.arjuna.ats.arjuna.common.ObjectStoreEnvironmentBean;
import com.arjuna.ats.arjuna.common.Uid;
import com.arjuna.ats.arjuna.exceptions.ObjectStoreException;
import com.arjuna.ats.arjuna.logging.tsLogger;
import com.arjuna.ats.arjuna.objectstore.StateStatus;
import com.arjuna.ats.arjuna.objectstore.jdbc.JDBCAccess;
import com.arjuna.ats.arjuna.state.InputObjectState;
import com.arjuna.ats.arjuna.state.OutputObjectState;
import com.arjuna.ats.internal.arjuna.common.UidHelper;

/**
 * An object store implementation which uses a JDBC database for maintaining
 * object states. All states are maintained within a single table.
 */

public abstract class JDBCImple_driver {

	// protected Connection connection;
	protected String tableName;
	private JDBCAccess jdbcAccess;
	
	private static final int BATCH_SIZE = 1000; 
	private static final List<Uid> BATCH_UIDS = new LinkedList<Uid>();

	public boolean commit_state(Uid objUid, String typeName)
			throws ObjectStoreException {
		// Taken this requirement from ObjStoreBrowser
		if (typeName.startsWith("/"))
			typeName = typeName.substring(1);

		boolean result = false;
		Connection connection = null;
		PreparedStatement pstmt = null;
		PreparedStatement pstmt2 = null;
		try {
			connection = jdbcAccess.getConnection();
			// Delete any previously committed state
			pstmt = connection
					.prepareStatement("DELETE FROM "
							+ tableName
							+ " WHERE UidString = ? AND TypeName = ? AND StateType = ?");

			pstmt.setString(1, objUid.stringForm());
			pstmt.setString(2, typeName);
			pstmt.setInt(3, 1);

			int rowcount = pstmt.executeUpdate();
			if (rowcount > 0) {
				tsLogger.i18NLogger
						.trace_JDBCImple_previouslycommitteddeleted(rowcount);
			}

			// now do the commit itself:
			pstmt2 = connection
					.prepareStatement("UPDATE "
							+ tableName
							+ " SET StateType = 1 WHERE UidString = ? AND TypeName = ? AND StateType = "
							+ StateStatus.OS_UNCOMMITTED);

			pstmt2.setString(1, objUid.stringForm());
			pstmt2.setString(2, typeName);

			int rowcount2 = pstmt2.executeUpdate();
			if (rowcount2 > 0) {
				connection.commit();
				result = true;
			} else {
				connection.rollback();
			}

		} catch (Exception e) {
			tsLogger.i18NLogger.warn_objectstore_JDBCImple_writefailed(e);
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					// Ignore
				}
			}
			if (pstmt2 != null) {
				try {
					pstmt2.close();
				} catch (SQLException e) {
					// Ignore
				}
			}
			if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // Ignore
                }
			}
		}

		return result;
	}

	public boolean hide_state(Uid objUid, String typeName)
			throws ObjectStoreException {
		// Taken this requirement from ObjStoreBrowser
		if (typeName.startsWith("/"))
			typeName = typeName.substring(1);
		boolean result = false;

		Connection connection = null;
		PreparedStatement pstmt = null;
		try {
			connection = jdbcAccess.getConnection();
			pstmt = connection
					.prepareStatement("UPDATE "
							+ tableName
							+ " SET Hidden = 1 WHERE UidString = ? AND TypeName = ? AND Hidden = 0");

			pstmt.setString(1, objUid.stringForm());
			pstmt.setString(2, typeName);

			int rowcount = pstmt.executeUpdate();
			connection.commit();
			if (rowcount > 0) {
				result = true;
			}
		} catch (Exception e) {
			tsLogger.i18NLogger.warn_objectstore_JDBCImple_1(e);
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					// Ignore
				}
			}
			if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // Ignore
                }
			}
		}

		return result;
	}

	public boolean reveal_state(Uid objUid, String typeName)
			throws ObjectStoreException {
		// Taken this requirement from ObjStoreBrowser
		if (typeName.startsWith("/"))
			typeName = typeName.substring(1);
		boolean result = false;

		Connection connection = null;
        PreparedStatement pstmt = null;
		try {
			connection = jdbcAccess.getConnection();
			pstmt = connection
					.prepareStatement("UPDATE "
							+ tableName
							+ " SET Hidden = 0 WHERE UidString = ? AND TypeName = ? AND Hidden = 1");

			pstmt.setString(1, objUid.stringForm());
			pstmt.setString(2, typeName);

			int rowcount = pstmt.executeUpdate();
			connection.commit();
			if (rowcount > 0) {
				result = true;
			}
		} catch (Exception e) {
			tsLogger.i18NLogger.warn_objectstore_JDBCImple_2(e);
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					// Ignore
				}
			}
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
		}

		return result;
	}

	/**
	 * currentState - determine the current state of an object. State search is
	 * ordered OS_UNCOMMITTED, OS_UNCOMMITTED_HIDDEN, OS_COMMITTED,
	 * OS_COMMITTED_HIDDEN
	 * 
	 * @throws ObjectStoreException - in case the JDBC store cannot be contacted
	 */
	public int currentState(Uid objUid, String typeName)
			throws ObjectStoreException {
		// Taken this requirement from ObjStoreBrowser
		if (typeName.startsWith("/"))
			typeName = typeName.substring(1);
		int theState = StateStatus.OS_UNKNOWN;
		ResultSet rs = null;

		Connection connection = null;
        PreparedStatement pstmt = null;
		try {
			connection = jdbcAccess.getConnection();
			pstmt = connection
					.prepareStatement("SELECT StateType, Hidden FROM "
							+ tableName
							+ " WHERE UidString = ? AND TypeName = ?");

			pstmt.setString(1, objUid.stringForm());
			pstmt.setString(2, typeName);

			rs = pstmt.executeQuery();

			// we may have multiple states. need to sort out the order of
			// precedence
			// without making multiple round trips out to the db. this gets
			// a bit messy:

			boolean have_OS_UNCOMMITTED = false;
			boolean have_OS_COMMITTED = false;
			boolean have_OS_UNCOMMITTED_HIDDEN = false;
			boolean have_OS_COMMITTED_HIDDEN = false;

			while (rs.next()) {
				int stateStatus = rs.getInt(1);
				int hidden = rs.getInt(2);

				switch (stateStatus) {
				case StateStatus.OS_UNCOMMITTED:
					if (hidden == 0)
						have_OS_UNCOMMITTED = true;
					else
						have_OS_UNCOMMITTED_HIDDEN = true;
					break;
				case StateStatus.OS_COMMITTED:
					if (hidden == 0)
						have_OS_COMMITTED = true;
					else
						have_OS_COMMITTED_HIDDEN = true;
					break;
				}
			}
			connection.commit();

			// examine in reverse order:
			if (have_OS_COMMITTED_HIDDEN) {
				theState = StateStatus.OS_COMMITTED_HIDDEN;
			}
			if (have_OS_COMMITTED) {
				theState = StateStatus.OS_COMMITTED;
			}
			if (have_OS_UNCOMMITTED_HIDDEN) {
				theState = StateStatus.OS_UNCOMMITTED_HIDDEN;
			}
			if (have_OS_UNCOMMITTED) {
				theState = StateStatus.OS_UNCOMMITTED;
			}
		} catch (Exception e) {
			tsLogger.i18NLogger.warn_objectstore_JDBCImple_3(e);
			throw new ObjectStoreException(e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore
				}
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					// Ignore
				}
			}
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
		}

		return theState;
	}

	/**
	 * allObjUids - Given a type name, return an ObjectState that contains all
	 * of the uids of objects of that type.
	 */
	public boolean allObjUids(String typeName, InputObjectState state, int match)
			throws ObjectStoreException {
		// Taken this requirement from ObjStoreBrowser
		if (typeName.startsWith("/"))
			typeName = typeName.substring(1);
		try {
			OutputObjectState store = new OutputObjectState();

			Connection connection = jdbcAccess.getConnection();
			Statement stmt = connection.createStatement();
			ResultSet rs = null;

			try {
				/*
				 * Not used enough to warrant a PreparedStatement.
				 */
				rs = stmt.executeQuery("SELECT DISTINCT UidString FROM "
						+ tableName + " WHERE TypeName = '" + typeName + "'");

				boolean finished = false;

				while (!finished && rs.next()) {
					Uid theUid = null;

					try {
						theUid = new Uid(rs.getString(1));
						UidHelper.packInto(theUid, store);
					} catch (IOException ex) {
						tsLogger.i18NLogger.warn_objectstore_JDBCImple_5(ex);

						return false;
					}
				}
				connection.commit();
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						// Ignore
					}
				}
				if (stmt != null) {
					try {
						stmt.close();
					} catch (SQLException e) {
						// Ignore
					}
				}
				if (connection != null) {
					try {
						connection.close();
					} catch (SQLException e) {
						// Ignore
					}
				}
			}

			UidHelper.packInto(Uid.nullUid(), store);

			state.setBuffer(store.buffer());

			store = null;

			return true;
		} catch (Exception e) {
			tsLogger.i18NLogger.warn_objectstore_JDBCImple_4(e);
			return false;
		}
	}

	public boolean allTypes(InputObjectState foundTypes)
			throws ObjectStoreException {

		try {
			OutputObjectState store = new OutputObjectState();

			Connection connection = jdbcAccess.getConnection();
			Statement stmt = connection.createStatement();
			ResultSet rs = null;

			try {
				/*
				 * Not used enough to warrant a PreparedStatement.
				 */
				rs = stmt.executeQuery("SELECT DISTINCT TypeName FROM "
						+ tableName);

				boolean finished = false;

				while (!finished && rs.next()) {
					try {
						String type = rs.getString(1);
						store.packString(type);
					} catch (IOException ex) {
						tsLogger.i18NLogger.warn_objectstore_JDBCImple_7(ex);

						return false;
					}
				}

				connection.commit();
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						// Ignore
					}
				}
				if (stmt != null) {
					try {
						stmt.close();
					} catch (SQLException e) {
						// Ignore
					}
				}
				if (connection != null) {
					try {
						connection.close();
					} catch (SQLException e) {
						// Ignore
					}
				}
			}

			store.packString("");

			foundTypes.setBuffer(store.buffer());
			return true;
		} catch (Exception e) {
			tsLogger.i18NLogger.warn_objectstore_JDBCImple_6(e);
			return false;
		}
	}

	public boolean remove_state(Uid objUid, String typeName, int stateType)
			throws ObjectStoreException {
		// Taken this requirement from ObjStoreBrowser
		if (typeName.startsWith("/"))
			typeName = typeName.substring(1);
		boolean result = false;

		if (typeName != null) {
			if ((stateType == StateStatus.OS_COMMITTED)
					|| (stateType == StateStatus.OS_UNCOMMITTED)) {
					
				if (BATCH_UIDS.size() < BATCH_SIZE - 1) {// add Uids into batch list until it's size reached predefined BATCH_SIZE
					BATCH_UIDS.add(objUid);
					tsLogger.logger.infof("ADDING UID %s %s %s", objUid.stringForm(), stateType, typeName);
				} else {
					BATCH_UIDS.add(objUid);	
				    Connection connection = null;
			        PreparedStatement pstmt = null;

					try {
						connection = jdbcAccess.getConnection();
						
						StringBuffer buffer = new StringBuffer();
	    				for (int i = 0; i < BATCH_UIDS.size(); i++) {
	    					buffer.append("?,");
	    				}
	    				// batch delete the collected Uids
						pstmt = connection
								.prepareStatement("DELETE FROM "
										+ tableName
										+ " WHERE UidString in ( "
										+ buffer.substring(0, buffer.length() - 1)
										+ " ) AND TypeName = ? AND StateType = ?");
						
						int index = 0;
						for (Uid uid : BATCH_UIDS) {
							pstmt.setString(++index, uid.stringForm());
						}
						
						pstmt.setString(index + 1, "StateManager/BasicAction/TwoPhaseCoordinator/AtomicAction");
						pstmt.setInt(index + 2, 1);
	
						tsLogger.logger.infof("DELETE WHERE");
	
						if (pstmt.executeUpdate() > 0) {
							result = true;
						}
	
						connection.commit();
						tsLogger.logger.infof("DELETE FINISHED %s", result);
					} catch (Exception e) {
						result = false;
						tsLogger.i18NLogger.warn_objectstore_JDBCImple_8(e);
					} finally {
						if (pstmt != null) {
							try {
								pstmt.close();
							} catch (SQLException e) {
								// Ignore
								tsLogger.logger.errorf(e, "Error DELETING");
							}
						}
			            if (connection != null) {
			                try {
			                    connection.close();
			                } catch (SQLException e) {
			                    // Ignore
			                	tsLogger.logger.errorf(e, "Error CLOSING");
			                }
			            }
					}
					//clear the Uids list as all were deleted by batch
					BATCH_UIDS.clear();
				}
			} else {
				// can only remove (UN)COMMITTED objs
				tsLogger.i18NLogger.warn_objectstore_JDBCImple_9(
						Integer.toString(stateType), objUid);
			}
		} else {
			tsLogger.i18NLogger.warn_objectstore_JDBCImple_10(objUid);
		}

		return result;
	}

	public InputObjectState read_state(Uid objUid, String typeName,
			int stateType) throws ObjectStoreException {
		InputObjectState result = null;
		// Taken this requirement from ObjStoreBrowser
		if (typeName.startsWith("/"))
			typeName = typeName.substring(1);

		if ((stateType == StateStatus.OS_COMMITTED)
				|| (stateType == StateStatus.OS_UNCOMMITTED)) {
			ResultSet rs = null;

			Connection connection = null;
	        PreparedStatement pstmt = null;
			try {
				connection = jdbcAccess.getConnection();
				pstmt = connection
						.prepareStatement("SELECT ObjectState FROM "
								+ tableName
								+ " WHERE UidString = ? AND TypeName = ? AND StateType = ?");
				pstmt.setString(1, objUid.stringForm());
				pstmt.setString(2, typeName);
				pstmt.setInt(3, stateType);

				rs = pstmt.executeQuery();
				if (rs.next()) {

					byte[] buffer = rs.getBytes(1);

					if (buffer != null) {
						result = new InputObjectState(objUid, typeName, buffer);
					} else {
						tsLogger.i18NLogger
								.warn_objectstore_JDBCImple_readfailed();
					}
				}

				connection.commit();
			} catch (Exception e) {
				result = null;
				tsLogger.i18NLogger.warn_objectstore_JDBCImple_14(e);
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						// Ignore
					}
				}
				if (pstmt != null) {
					try {
						pstmt.close();
					} catch (SQLException e) {
						// Ignore
					}
				}
	            if (connection != null) {
	                try {
	                    connection.close();
	                } catch (SQLException e) {
	                    // Ignore
	                }
	            }
			}
		}

		return result;
	}

	public boolean write_state(Uid objUid, String typeName,
			OutputObjectState state, int stateType) throws ObjectStoreException {
		// Taken this requirement from ObjStoreBrowser
		if (typeName.startsWith("/"))
			typeName = typeName.substring(1);
		boolean result = false;

		int imageSize = (int) state.length();

		if (imageSize > getMaxStateSize()) {
			tsLogger.i18NLogger.warn_objectstore_JDBCImple_over_max_image_size(
					imageSize, getMaxStateSize());
		} else if (imageSize > 0) {
			byte[] b = state.buffer();
			ResultSet rs = null;

			Connection connection = null;
	        PreparedStatement pstmt = null;
			try {
				connection = jdbcAccess.getConnection();
				pstmt = connection
						.prepareStatement(
								"SELECT ObjectState, UidString, StateType, TypeName FROM "
										+ tableName
										+ " WHERE UidString = ? AND StateType = ? AND TypeName = ? FOR UPDATE",
								ResultSet.TYPE_FORWARD_ONLY,
								ResultSet.CONCUR_UPDATABLE);

				pstmt.setString(1, objUid.stringForm());
				pstmt.setInt(2, stateType);
				pstmt.setString(3, typeName);

				rs = pstmt.executeQuery();

				if (rs.next()) {
				    PreparedStatement pstmt2 = connection
                        .prepareStatement("UPDATE " + tableName +
                            " SET ObjectState = ?" +
                           " WHERE StateType=? AND TypeName=? AND UidString=?");
                    try {
                        pstmt2.setBytes(1, b);
                        pstmt2.setInt(2, stateType);
                        pstmt2.setString(3, typeName);
                        pstmt2.setString(4, objUid.stringForm());
                        pstmt2.executeUpdate();
                    } finally {
                        pstmt2.close();
                    }
				} else {
					connection.commit();
					// not in database, do insert:
					PreparedStatement pstmt2 = connection
							.prepareStatement("INSERT INTO "
									+ tableName
									+ " (StateType,Hidden,TypeName,UidString,ObjectState) VALUES (?,0,?,?,?)");
					try {
    					pstmt2.setInt(1, stateType);
    					pstmt2.setString(2, typeName);
    					pstmt2.setString(3, objUid.stringForm());
    					pstmt2.setBytes(4, b);
    
    					pstmt2.executeUpdate();
					} finally {
					    pstmt2.close();
					}
				}

				connection.commit();
				result = true;
			} catch (Exception e) {
				tsLogger.i18NLogger.warn_objectstore_JDBCImple_writefailed(e);
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						// Ignore
					}
				}
				if (pstmt != null) {
					try {
						pstmt.close();
					} catch (SQLException e) {
						// Ignore
					}
				}
	            if (connection != null) {
	                try {
	                    connection.close();
	                } catch (SQLException e) {
	                    // Ignore
	                }
	            }
			}

		}

		return result;
	}

	/**
	 * Set up the store for use.
	 * 
	 * @throws NamingException
     * @throws SQLException In case the configured store cannot be connected to
	 */
	public void initialise(final JDBCAccess jdbcAccess, String tableName,
			ObjectStoreEnvironmentBean jdbcStoreEnvironmentBean)
			throws SQLException, NamingException {
		this.jdbcAccess = jdbcAccess;

		// connection = new ThreadLocal<Connection>() {
		//
		// @Override
		// protected Connection initialValue() {
		// try {
		// return jdbcAccess.getConnection();
		// } catch (Exception e) {
		// throw new RuntimeException(e);
		// }
		// }
		// };

		try (Connection connection = jdbcAccess.getConnection()) {

    		try (Statement stmt = connection.createStatement()) {
    
        		// table [type, object UID, format, blob]
        
        		if (jdbcStoreEnvironmentBean.getDropTable()) {
        			try {
        				stmt.executeUpdate("DROP TABLE " + tableName);
        			} catch (SQLException ex) {
        				checkDropTableException(connection, ex);
        			}
        		}
        
        		if (jdbcStoreEnvironmentBean.getCreateTable()) {
        			try {
        				createTable(stmt, tableName);
        			} catch (SQLException ex) {
        				checkCreateTableError(ex);
        			}
        		}
        
        		// This can be the case when triggering via EmptyObjectStore
        		if (!connection.getAutoCommit()) {
        			connection.commit();
        		}
            }
		}

		this.tableName = tableName;
	}

	/**
	 * Can be overridden by implementation-specific code to create the store
	 * table. Called from initialise() and addTable(), above.
	 */
	protected void createTable(Statement stmt, String tableName)
			throws SQLException {
		String statement = "CREATE TABLE "
				+ tableName
				+ " (StateType INTEGER NOT NULL, Hidden INTEGER NOT NULL, "
				+ "TypeName VARCHAR(255) NOT NULL, UidString VARCHAR(255) NOT NULL, ObjectState "
				+ getObjectStateSQLType()
				+ ", PRIMARY KEY(UidString, TypeName, StateType))";
		stmt.executeUpdate(statement);
	}

	protected String getObjectStateSQLType() {
		return "bytea";
	}

	protected abstract void checkCreateTableError(SQLException ex)
			throws SQLException;

	protected abstract void checkDropTableException(Connection connection, SQLException ex)
			throws SQLException;

	public int getMaxStateSize() {
		return 65535;
	}
}
