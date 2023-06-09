import unittest

from simpleRaft.memory_board import MemoryBoard
#from append_entries import AppendEntriesMessage
#from request_vote import RequestVoteMessage
from simpleRaft.server import Server
from simpleRaft.follower import Follower
from simpleRaft.candidate import Candidate
from simpleRaft.leader import Leader

class TestCandidateServer( unittest.TestCase ):

	def setUp( self ):
		board = MemoryBoard()
		state = Follower()
		self.oserver = Server( 0, state, [], board, [] )

		board = MemoryBoard()
		state = Candidate()
		self.server = Server( 1, state, [], board, [ self.oserver ] )

		self.oserver._neighbors.append( self.server )

	def test_candidate_server_had_intiated_the_election( self ):

		self.assertEquals( 1, len( self.oserver._messageBoard._board ) )

		self.oserver.on_message( self.oserver._messageBoard.get_message() )

		self.assertEquals( 1, len( self.server._messageBoard._board ) )
		self.assertEquals( True, self.server._messageBoard.get_message().data["response"] )

	def test_candidate_server_had_gotten_the_vote( self ):
		self.oserver.on_message( self.oserver._messageBoard.get_message() )

		self.assertEquals( 1, len( self.server._messageBoard._board ) )
		self.assertEquals( True, self.server._messageBoard.get_message().data["response"] )

	def test_candidate_server_wins_election( self ):
		board = MemoryBoard()
		state = Follower()
		server0 = Server( 0, state, [], board, [] )

		board = MemoryBoard()
		state = Follower()
		server1 = Server( 1, state, [], board, [] )

		board = MemoryBoard()
		state = Candidate()
		server2 = Server( 2, state, [], board, [ server1, server0 ] )

		server0._neighbors.append( server2 )
		server1._neighbors.append( server2 )

		server1.on_message( server1._messageBoard.get_message() )
		server0.on_message( server0._messageBoard.get_message() )

		server2._total_nodes = 3

		server2.on_message( server2._messageBoard.get_message() )
		server2.on_message( server2._messageBoard.get_message() )

		self.assertEquals( type( server2._state ), Leader )

	def test_two_candidates_tie( self ):
		followers = []

		for i in range( 4 ):
			board = MemoryBoard()
			state = Follower()
			followers.append( Server( i, state, [], board, [] ) )

		board = MemoryBoard()
		state = Candidate()
		c0 = Server( 5, state, [], board, followers[0:2] )

		board = MemoryBoard()
		state = Candidate()
		c1 = Server( 6, state, [], board, followers[2:] )

		for i in range( 2 ):
			followers[i]._neighbors.append( c0 )
			followers[i].on_message( followers[i]._messageBoard.get_message() )

		for i in range( 2, 4 ):
			followers[i]._neighbors.append( c1 )
			followers[i].on_message( followers[i]._messageBoard.get_message() )

		c0._total_nodes = 6
		c1._total_nodes = 6

		for i in range( 2 ):
			c0.on_message( c0._messageBoard.get_message() )
			c1.on_message( c1._messageBoard.get_message() )

		self.assertEquals( type( c0._state ), Candidate )
		self.assertEquals( type( c1._state ), Candidate )

	def test_two_candidates_one_wins( self ):
		followers = []

		for i in range( 6 ):
			board = MemoryBoard()
			state = Follower()
			followers.append( Server( i, state, [], board, [] ) )

		board = MemoryBoard()
		state = Candidate()
		c0 = Server( 7, state, [], board, followers[0:2] )

		board = MemoryBoard()
		state = Candidate()
		c1 = Server( 8, state, [], board, followers[2:] )

		for i in range( 2 ):
			followers[i]._neighbors.append( c0 )
			followers[i].on_message( followers[i]._messageBoard.get_message() )

		for i in range( 2, 6 ):
			followers[i]._neighbors.append( c1 )
			followers[i].on_message( followers[i]._messageBoard.get_message() )

		c0._total_nodes = 7
		c1._total_nodes = 7

		for i in range( 2 ):
			c0.on_message( c0._messageBoard.get_message() )
			
		for i in range( 4 ):
			c1.on_message( c1._messageBoard.get_message() )

		self.assertEquals( type( c0._state ), Candidate )
		self.assertEquals( type( c1._state ), Leader )

	def test_candidate_fails_to_win_election_so_resend_request( self ):
		pass

	def test_multiple_candidates_fail_to_win_so_resend_requests( self ):
		pass


TestCandidateServer().test_candidate_server_wins_election()