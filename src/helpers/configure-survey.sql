select f.id,f.name,b.name,b.repeat,q.id,q.name,q.type
from frm_questions as q
	inner join frm_blocks as b on q.block = b.id
    inner join frm_blocks_forms as bf on bf.block = b.id
    inner join frm_forms as f on bf.form = f.id;