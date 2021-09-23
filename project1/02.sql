select CatchedPokemon.nickname from CatchedPokemon join Pokemon on CatchedPokemon.pid = Pokemon.id where CatchedPokemon.level >= 50 order by CatchedPokemon.nickname;
