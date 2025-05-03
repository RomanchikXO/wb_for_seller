from django.shortcuts import render, redirect
from .forms import RegistrationForm
from django.contrib.auth.hashers import check_password
from .models import CustomUser


def register_view(request):
    if request.method == 'POST':
        form = RegistrationForm(request.POST)
        if form.is_valid():
            user = form.save(commit=False)
            user.set_password(form.cleaned_data['password'])
            user.save()
            return redirect('login')  # пока без реализации логина
    else:
        form = RegistrationForm()
    return render(request, 'register.html', {'form': form})


def login_view(request):
    request.session.flush()
    message = ''
    next_url = request.GET.get('next') or request.POST.get('next')

    if request.method == 'POST':
        name = request.POST.get('name')
        password = request.POST.get('password')

        try:
            user = CustomUser.objects.get(name=name)
            if user and check_password(password, user.password):
                if not user.groups:
                    message = 'Ожидайте...'
                else:
                    # ✅ Сохраняем user_id в сессию
                    request.session['user_id'] = user.id
                    request.session['user_name'] = user.name
                    return redirect(next_url or '/main/')
            else:
                message = 'Неверный пароль'
        except CustomUser.DoesNotExist:
            message = 'Пользователь не найден'

    return render(request, 'login.html', {'message': message})