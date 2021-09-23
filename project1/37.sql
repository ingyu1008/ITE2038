select sum(CatchedPokemon.level) from CatchedPokemon join Pokemon on CatchedPokemon.pid = Pokemon.id where Pokemon.type = 'Fire';
