select Trainer.name, sum(CatchedPokemon.level) from Trainer join CatchedPokemon on Trainer.id = CatchedPokemon.owner_id group by Trainer.id order by sum(CatchedPokemon.level) desc limit 1;
