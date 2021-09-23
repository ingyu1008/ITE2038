select count(*) from CatchedPokemon join Pokemon on CatchedPokemon.pid = Pokemon.id group by Pokemon.type order by Pokemon.type;
