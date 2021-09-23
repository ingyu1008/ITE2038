select sum(CatchedPokemon.level) from CatchedPokemon join Trainer on CatchedPokemon.owner_id = Trainer.id where Trainer.name = 'Matis';
