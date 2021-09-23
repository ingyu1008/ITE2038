select cp.nickname from CatchedPokemon as cp join Trainer as t on cp.owner_id = t.id where cp.level = (select max(CatchedPokemon.level) from CatchedPokemon join Trainer on CatchedPokemon.owner_id = Trainer.id where Trainer.hometown = t.hometown)
