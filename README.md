# Propósito do Case

Intuito desse case é apresentar uma forma de armazenar arquivos em um Data Lake. Foi utilizado um ambiente Cloud para  a viabilização dessa solução.

Principal proposta é conseguirmos realizar o download de vídeos de mídias sociais, transcrever esses áudios e armazená-los em um ADLS para utilização posterior em análises de sentimento e impacto dos vídeos em cima da imagem do Santander.

# Arquitetura do Projeto
![enter image description here](https://raw.githubusercontent.com/GuhBrando/audio-transcription/main/Desenho%20de%20Arquitetura.png)
Imagem 1 – Arquitetura do Projeto

Acima vemos a arquitetura do projeto, nela esta separada em diversos retângulos cujo cada um tem seu proposito e finalidade.

No primeiro retângulo estão todos os objetos que remetem a configuração do ambiente, instalação dos ambientes e utilização de dependência e credencias, tais como instalação do codec “ffmpeg”, instalação de driver de som “pulseaudio-equalizer” para que a transcrição funcione conforme previsto e importe de todas as credenciais para ter acesso ao Storage Account e ter as devidas permissões para rodar os Jobs do databricks.

No segundo retângulo temos todas as tecnologias que poderão ser visadas pela tecnologia, como tiktok, youtube e tiktok. Para essa primeira versão foi desenvolvido apenas para o ambiente do youtube.

No terceiro retângulo temos um dos motores principais. Ele quem fará o trabalho de coletar todos os vídeos do youtube, na stage – que no nosso caso será o ambiente do databricks – e depois de baixar os arquivos .webm, convertê-los para o formato .wav, formato que a transcrição de áudio aceita.

Nos objetos acima, temos a apresentação da camada bronze, que no nosso caso é um storage account na Azure, um container no ADLS.
