select avg(CatchedPokemon.level) from Trainer join CatchedPokemon on Trainer.id = CatchedPokemon.owner_id where Trainer.name = 'Red';
