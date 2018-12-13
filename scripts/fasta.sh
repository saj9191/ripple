#!/bin/bash

fastas=( Arabidopsis Boar Cattle Ecoli FruitFly Human Mouse Rat RedJunglefowl Rice Roundworm Trypanosoma Tuberculosis Yeast Zebrafish )
components=( auxlocs protix pepix )

rm fasta
rm -rf crux-output
rm -rf fasta.index


process()
{
	name=$1
  features=$2
  for i in "${fastas[@]}"
  do
    s3cmd get s3://maccoss-fasta/normal$i/fasta
	  cmd="./crux tide-index fasta fasta.index $features --overwrite T"
		echo $cmd
	  eval $cmd
	  s3cmd put fasta s3://maccoss-fasta/$name$i/fasta
		for j in "${components[@]}"
		do
      s3cmd put fasta.index/$j s3://maccoss-fasta/$name$i/$j
		done
    rm fasta
    rm -rf crux-output
    rm -rf fasta.index
  done
}


#process "superSilacLys4Arg6Lys8Arg10" "--mods-spec 1K+4.025107 --mods-spec 1R+6.020129 --mods-spec 1K+8.014199 --mods-spec 1R+10.008269"
#process "silacLys8Arg10" "--mods-spec 1K+8.014199 --mods-spec 1R+10.008269"
process "silacLys6Arg6" "--mods-spec 1KR+6.020129"
#process "normal" "--missed-cleavages 1 --mods-spec 1M+15.9949"
#process "itraq4" "--missed-cleavages 1  --mods-spec 1M+15.9949 --mods-spec K+144.10253 --nterm-peptide-mods-spec X+144.10253"
#process "itraq8" "--missed-cleavages 1  --mods-spec 1M+15.9949 --mods-spec K+304.2022 --nterm-peptide-mods-spec X+304.2022"
#process "itraq2" "--missed-cleavages 1  --mods-spec 1M+15.9949 --mods-spec K+225.155833 --nterm-peptide-mods-spec X+225.155833"
#process "tmt6" "--missed-cleavages 1 --mods-spec 1M+15.9949 --mods-spec K+229.162932 --nterm-peptide-mods-spec X+229.162932"
#process "silac8" "--missed-cleavages 1 --mods-spec 1M+15.9949 --mods-spec 2K+8.014199"
#process "itraq8" "--missed-cleavages 1 --mods-spec 1M+15.9949 --mods-spec K+304.2022 --nterm-peptide-mods-spec X+304.2022"
#process "itraq2" "--missed-cleavages 1 --mods-spec 1M+15.9949 --mods-spec K+225.155833 --nterm-peptide-mods-spec X+225.155833"
#process "silac4" "--missed-cleavages 1 --mods-spec 1M+15.9949 --mods-spec 2R+3.988140"
#process "phosphorylation" "--mods-spec 1STY+79.966331"
#process "phosphorylationCleav" "--missed-cleavages 1 --mods-spec 1STY+79.966331"
#process "itraq4Phosphorylation" "--missed-cleavages 1  --mods-spec 1STY+79.966331 --mods-spec K+144.10253 --nterm-peptide-mods-spec X+144.10253"
#process "itraq8Phosphorylation" "--missed-cleavages 1  --mods-spec 1STY+79.966331 --mods-spec K+304.2022 --nterm-peptide-mods-spec X+304.2022"
#process "itraq2Phosphorylation" "--missed-cleavages 1  --mods-spec 1STY+79.966331 --mods-spec K+225.155833 --nterm-peptide-mods-spec X+225.155833"
#process "tmt6Phosphorylation" "--missed-cleavages 1  --mods-spec 1STY+79.966331 --mods-spec K+229.162932 --nterm-peptide-mods-spec X+229.162932"
#return


#process "normal" "--missed-cleavages 1 --mods-spec 1M+15.9949"
#process "silac8" "--missed-cleavages 1 --mods-spec 1M+15.9949 --mods-spec 2K+8.014199"
#process "itraq8" "--missed-cleavages 1 --mods-spec 1M+15.9949 --mods-spec K+304.2022 --nterm-peptide-mods-spec X+304.2022"
#process "itraq2" "--missed-cleavages 1 --mods-spec 1M+15.9949 --mods-spec K+225.155833 --nterm-peptide-mods-spec X+225.155833"
#process "tmt6" "--missed-cleavages 1 --mods-spec 1M+15.9949 --mods-spec K+229.162932 --nterm-peptide-mods-spec X+229.162932"
#process "silac4" "--missed-cleavages 1 --mods-spec 1M+15.9949 --mods-spec 2R+3.988140"


