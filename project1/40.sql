select Trainer.name from Trainer join CatchedPokemon on Trainer.id = CatchedPokemon.owner_id join Pokemon on Pokemon.id = CatchedPokemon.pid where Pokemon.name = 'Pikachu' order by Trainer.name;
