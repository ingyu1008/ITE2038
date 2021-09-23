select Pokemon.name from Pokemon join CatchedPokemon on Pokemon.id = CatchedPokemon.pid join Gym on Gym.leader_id = CatchedPokemon.owner_id where Gym.city = 'Rainbow City' order by Pokemon.name;
