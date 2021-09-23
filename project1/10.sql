select Pokemon.name from Pokemon where Pokemon.id not in (select pid from CatchedPokemon) order by Pokemon.name;
